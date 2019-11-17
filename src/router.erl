-module(router).
-export([start/1]).


debug() -> nope. % 'debug' for debug mode, else anything


start (RouterName) ->
   spawn(fun() -> init(RouterName) end).


init(RouterName) ->
   RoutingTable = ets:new(undef, [private]),
   OtherTable = ets:new(undef, [private]),
   ets:insert(RoutingTable, {'$NoInEdges', 0}),
   loop(RouterName, RoutingTable, OtherTable).


loop (RouterName, RoutingTable, OtherTable) ->
   receive
      {message, Dest, From, Pid, Trace} ->
         handleMessage(RouterName, RoutingTable, Dest, From, Pid, Trace),
         loop(RouterName, RoutingTable, OtherTable);
      {control, From, Pid, SeqNum, ControlFun} -> % From =:= Pid => from controller
         io:format("~n      Router ~p enter control with SeqNum ~p~n", [RouterName, SeqNum]),
         NewRoutingTable = handleControl(RouterName, RoutingTable, OtherTable, From, Pid, SeqNum, ControlFun),
         io:format("~n~n      Router ~p return from control~n", [RouterName]),
         loop(RouterName, NewRoutingTable, OtherTable);
      {dump, From} ->
         handleDump(RoutingTable, From),
         loop(RouterName, RoutingTable, OtherTable);
      stop ->
         handleStop(RouterName, RoutingTable, OtherTable);
      {'2pcPhase2', _, _, _} -> % Swallow up any extra 2pc messages
         loop(RouterName, RoutingTable, OtherTable);
      X ->
         case debug() of debug -> io:format("~p: Main loop swallowed message => ~p~n", [RouterName, X]); _ -> ignore end,
         loop(RouterName, RoutingTable, OtherTable)
   end.


% % % % %
% Handlers
% % % % %
handleDump(RoutingTable, From) ->
   Dump = ets:match(RoutingTable, '$1'),
   case debug() of debug -> io:format("Taking a dump ~p~n", [Dump]); _ -> ignore end,
   From ! {table, self(), Dump}.


handleStop(RouterName, RoutingTable, OtherTable) ->
   case debug() of debug -> io:format("Stopping router: ~p~n", [RouterName]); _ -> ignore end,
   ets:delete(RoutingTable),
   ets:delete(OtherTable).


handleMessage(RouterName, RoutingTable, Dest, _, Pid, Trace) ->
   NextTrace = [RouterName|Trace],
   case Dest of
      RouterName ->
         Pid ! {trace, self(), lists:reverse(NextTrace)};
      _ ->
         Next = ets:lookup(RoutingTable, Dest),
         case Next of
            [] ->
               %% Bad things have happened...
            case debug() of debug -> io:format("Router ~p has received a message to ~p it can't route~n", [RouterName, Dest]); _ -> ignore end;
            _ ->
               [{_, Next_Pid}] = Next,
               Next_Pid ! {message, Dest, self(), Pid, NextTrace}
         end
   end.


handleControl(RouterName, RoutingTable, OtherTable, _, _, 0, ControlFun) ->
    % Initialization shouldn't have any reason to fail, or have created any new nodes
   _ = ControlFun(RouterName, RoutingTable),
   ets:insert(OtherTable, {executed, [0]}),
   RoutingTable;
handleControl(RouterName, RoutingTable, OtherTable, From, Pid, SeqNum, ControlFun) ->
   % not init
   [{_, Executed}] = ets:lookup(OtherTable, executed),
   Guard = lists:member(SeqNum, Executed),
   case Guard of
      true ->
         RoutingTable;  % Sequence number already executed
      false ->
         ets:insert(OtherTable, {executed, [SeqNum|Executed]}),
         TentativeRoutingTable = ets:new(undef,[private]),
         % ets:match returns a a list of lists or tuples, we flatten it to get a list of tuples which we can insert in one go
         Dump = lists:flatten(ets:match(RoutingTable, '$1')),
         ets:insert(TentativeRoutingTable, Dump),
         start2PC(RouterName, RoutingTable, TentativeRoutingTable, From, Pid, SeqNum, ControlFun)
   end.


start2PC(RouterName, RoutingTable, TentativeRoutingTable, Pid, Pid, SeqNum, ControlFun) ->
   % coordinator of 2 phase commit
   % Broadcast the control to all nodes so that a) sequence nubmers are consistent and b) they expect the correct messages later
   io:format("~n      Router ~p is a coordinator for SeqNum ~p~n", [RouterName, SeqNum]),
   broadcast(RoutingTable, {control, self(), Pid, SeqNum, ControlFun}),
   case ControlFun(RouterName, TentativeRoutingTable) of
      abort ->
         % We already know we're going to abort, but first we coordinate phase 1 so that everyone is on the same page...Efficiency third
         case debug() of debug -> io:format("~n~n      ~p: abort from ControlFun~n~n", [RouterName]); _ -> ignore end,
         _ = coordinateFirstPhase(RoutingTable, SeqNum),
         broadcast(RoutingTable, {'2pcPhase2', abortChange, SeqNum, self()}),
         _ = coordinateSecondPhase(RoutingTable, SeqNum, confirmAbort),
         Pid ! {abort, self(), SeqNum},
         ets:delete(TentativeRoutingTable),
         RoutingTable;
      Children ->  % Will also capture empty list, in which case foreach does nothing later
         Phase1Response = coordinateFirstPhase(RoutingTable, SeqNum),
         case Phase1Response of
            phase1Commit ->  case debug() of debug -> io:format("~n~n      ~p: Signaling to network commitChange~n~n", [RouterName]); _ -> ignore end,
               broadcast(RoutingTable, {'2pcPhase2', commitChange, SeqNum, self()}),
               _ = coordinateSecondPhase(RoutingTable, SeqNum, confirmCommit),
               ets:delete(RoutingTable),
               Pid ! {committed, self(), SeqNum},
               TentativeRoutingTable;
            _ -> case debug() of debug -> io:format("~n~n      ~p: Signalling network abortChange~n~n", [RouterName]); _ -> ignore end,
               broadcast(RoutingTable, {'2pcPhase2', abortChange, SeqNum, self()}),
               _ = coordinateSecondPhase(RoutingTable, SeqNum, confirmAbort),
               lists:foreach(fun(C) -> exit(C, "commit aborted") end, Children),
               Pid ! {abort, self(), SeqNum},
               ets:delete(TentativeRoutingTable),
               RoutingTable
         end
   end;
start2PC(RouterName, RoutingTable, TentativeRoutingTable, From, Pid, SeqNum, ControlFun) ->
   % slave of 2 phase commit
   % Broadcast the control to all nodes so that a) sequence nubmers are consistent and b) they expect the correct messages later
   io:format("~n      Router ~p is a slave for SeqNum ~p~n", [RouterName, SeqNum]),
   broadcast(RoutingTable, {control, self(), Pid, SeqNum, ControlFun}),
   case ControlFun(RouterName, TentativeRoutingTable) of % ControlFun should catch it's own errors and return abort safely
      abort ->
         % ControlFun has failed somewhere so backpropagate abort, we rely on the coordinator to signal an abort to everyone
         case debug() of debug -> io:format("~n~n      ~p: abort from ControlFun~n~n", [RouterName]); _ -> ignore end,
         Phase1Result = participatePhase1(RoutingTable, SeqNum, mustAbort),
         case Phase1Result of canCommit -> case debug() of debug -> io:format("~n~n   Router ~p asked to commit but signaled abort~n", [RouterName]); _ -> ignore end; _ -> ignore end,
         From ! {'2pcPhase1', Phase1Result, SeqNum, self()},
         Phase2Response = participatePhase2(SeqNum, true),
         case Phase2Response of commitChange -> case debug() of debug -> io:format("~n~n   Router ~p asked to commit but signaled abort~n", [RouterName]); _ -> ignore end; _ -> ignore end,
         broadcast(RoutingTable, {'2pcPhase2', abortChange, SeqNum, self()}),
         AckResult = participateInAcks(RoutingTable, SeqNum, confirmAbort),
         case AckResult of confirmCommit -> case debug() of debug -> io:format("~n~n   Router ~p asked to commit but signaled abort~n", [RouterName]); _ -> ignore end; _ -> ignore end,
         From ! {'2pcPhase2', confirmAbort, SeqNum, self()},
         ets:delete(TentativeRoutingTable),
         RoutingTable;
      Children ->  % Will also capture empty list, in which case foreach does nothing later
         Phase1Result = participatePhase1(RoutingTable, SeqNum, canCommit),
         From ! {'2pcPhase1', Phase1Result, SeqNum, self()},
         Phase2Response = participatePhase2(SeqNum, false),
         broadcast(RoutingTable, {'2pcPhase2', Phase2Response, SeqNum, self()}),
         case Phase2Response of
            commitChange -> case debug() of debug -> io:format("~n~n      ~p: Network signalled commitChange~n~n", [RouterName]); _ -> ignore end,
               participateInAcks(RoutingTable, SeqNum, confirmCommit),
               From ! {'2pcPhase2', confirmCommit, SeqNum, self()},
               ets:delete(RoutingTable),
               TentativeRoutingTable;
            abortChange -> case debug() of debug -> io:format("~n~n      ~p: Network signalled abortChange~n~n", [RouterName]); _ -> ignore end,
               lists:foreach(fun(C) -> exit(C, "commit aborted") end, Children),
               participateInAcks(RoutingTable, SeqNum, confirmAbort),
               From ! {'2pcPhase2', confirmAbort, SeqNum, self()},
               ets:delete(TentativeRoutingTable),
               RoutingTable
         end
   end.
% % % % %
% End Handlers
% % % % %

% % % % %
% Helpers
% % % % %
broadcast(RoutingTable, Msg) ->
   % We need to filter out the NoInEdges which is stored here... Luckily the atom is knowm :)
   Hops = [Hop || [{Dest, Hop}] <- ets:match(RoutingTable, '$1'), Dest =/= '$NoInEdges'],
   Unique = lists:usort(Hops), % usort will return a unique list, since every node will broadcast we only need to send a message once to each node
   lists:foreach(
      fun(Hop) ->
         case debug() of debug -> io:format("~nSending ~p to ~p~n", [Msg, Hop]); _ -> ignore end,
         Hop ! Msg
      end, Unique).


participatePhase1(RoutingTable, SeqNum, MyResponse) ->
   Hops = [Hop || [{Dest, Hop}] <- ets:match(RoutingTable, '$1'), Dest =/= '$NoInEdges'],
   Neighbours = length(lists:usort(Hops)), % usort returns unique entries i.e. neighbours
   Phase1Result = phase1MessageLoop(Neighbours, 0, SeqNum, MyResponse),
   case Phase1Result of
      X when X =:= mustAbort; X =:= canCommit -> ignore;
      % match and catch unexpected messages
      X -> case debug() of debug -> io:format("Phase 1 participant received odd message -> ~p~n", [X]); _ -> ignore end
   end,
   Phase1Result.


phase1MessageLoop(ExpectedResponses, CurrentResponses, SeqNum, MyResponse) ->
   receive
      {'2pcPhase1', canCommit, CommitSeq, _} -> % Phase 1 back prop message
         case CommitSeq of
            SeqNum ->
               case CurrentResponses + 1 of
                  ExpectedResponses ->
                     MyResponse;
                  _ ->
                     phase1MessageLoop(ExpectedResponses, CurrentResponses + 1, SeqNum, MyResponse)
               end;
            _ -> % not this sequence, swallow it
               phase1MessageLoop(ExpectedResponses, CurrentResponses, SeqNum, MyResponse)
         end;
      {'2pcPhase1', mustAbort, CommitSeq, _} -> % Phase 1 back prop message
         case CommitSeq of
            SeqNum ->
               case CurrentResponses + 1 of
                  ExpectedResponses ->
                     mustAbort;
                  _ ->  % Change response to can't commit (even if we can) since a neighbour can't commit
                     phase1MessageLoop(ExpectedResponses, CurrentResponses + 1, SeqNum, mustAbort)
               end;
            _ -> % not this sequence, swallow it
               phase1MessageLoop(ExpectedResponses, CurrentResponses, SeqNum, MyResponse)
         end;
      {control, From, _, SeqNum, _} ->
         From ! {'2pcPhase1', canCommit, SeqNum, self()},
         phase1MessageLoop(ExpectedResponses, CurrentResponses, SeqNum, MyResponse);
      {control, From, _, SeqNum2, _} -> % Control is for a different sequence... During this sequence... Abort both!!!
         From ! {'2pcPhase1', mustAbort, SeqNum2, self()}, % Abort the other sequence
         phase1MessageLoop(ExpectedResponses, CurrentResponses, SeqNum, mustAbort)
   after getTimeout() -> mustAbort
   end.


participatePhase2(SeqNum, MustAbort) ->
   Phase2Result = phase2MessageLoop(SeqNum, MustAbort),
   case Phase2Result of
      X when X =:= abortChange; X =:= commitChange -> ignore;
      % match and catch unexpected messages
      X -> case debug() of debug -> io:format("Phase 1 participant received odd message -> ~p~n", [X]); _ -> ignore end
   end,
   Phase2Result.


phase2MessageLoop(SeqNum, MustAbort) ->
   receive
      {'2pcPhase2', commitChange, CommitSeq, _} -> % Phase 2 broadcast message
         case CommitSeq of
            SeqNum ->
               case MustAbort of
                  true -> case debug() of debug -> io:format("~n~n      Router received commitChange, but has sent or seen an mustAbort~n~n"); _ -> ignore end;
                  _ -> ignored
               end,
               commitChange;
            _ -> % different sequence, swallow
               phase2MessageLoop(SeqNum, MustAbort)
         end;
   {'2pcPhase2', abortChange, CommitSeq, _} -> % Phase 2 broadcast message
      case CommitSeq of
         SeqNum ->
            abortChange;
         _ -> % different sequence, swallow
            phase2MessageLoop(SeqNum, MustAbort)
      end
   after getTimeout() -> % Phase 2 is not meant to timeout
      case debug() of debug -> io:format("~n~n     Controller timed out in 2nd phase of 2PC~n"); _ -> ignore end,
      phase2MessageLoop(SeqNum, MustAbort)
   end.


participateInAcks(RoutingTable, SeqNum, ExpectedReply) ->
   Hops = [Hop || [{Dest, Hop}] <- ets:match(RoutingTable, '$1'), Dest =/= '$NoInEdges'],
   Neighbours = length(lists:usort(Hops)), % usort returns unique entries i.e. neighbours
   AckResult = ackLoop(Neighbours, 0, SeqNum, ExpectedReply),
   case AckResult of
      X when X =:= confirmAbort; X =:= confirmCommit -> ignore;
      % match and catch unexpected messages
      X -> case debug() of debug -> io:format("Phase 1 participant received odd message -> ~p~n", [X]); _ -> ignore end
   end,
   AckResult.


ackLoop(ExpectedResponses, CurrentResponses, SeqNum, ExpectedReply) ->
   receive
      % We're in phase 2 so fewer checks are needed
      {'2pcPhase2', commitChange, _, From} ->
         From ! {'2pcPhase2', confirmCommit, SeqNum, self()},
         awaitPhase2Responses(ExpectedResponses, CurrentResponses, SeqNum, ExpectedReply);
      {'2pcPhase2', abortChange, _, From} ->
         From ! {'2pcPhase2', confirmAbort, SeqNum, self()},
         awaitPhase2Responses(ExpectedResponses, CurrentResponses, SeqNum, ExpectedReply);
      {'2pcPhase2', Reply, CommitSeq, _} -> % Phase 2 broadcast message
         case CommitSeq of
            SeqNum ->
               case Reply == ExpectedReply of
                  true -> ignore;  % as expected
                  false -> case debug() of debug -> io:format("2nd phase received reply ~p but expected => ~p~n", [Reply, ExpectedReply]); _ -> ignore end
               end,
               case CurrentResponses + 1 of
                  ExpectedResponses ->
                     ExpectedReply;
                  _ -> % still waiting on responses
                     ackLoop(ExpectedResponses, CurrentResponses + 1, SeqNum, ExpectedReply)
               end;
            _ -> % different sequence, swallow
               case debug() of debug -> io:format("Reply for sequence ~p but expected seq ~p~n", [CommitSeq, SeqNum]); _ -> ignore end,
               ackLoop(ExpectedResponses, CurrentResponses, SeqNum, ExpectedReply)
         end;
      X -> io:format("    ===> Slave received message ~p~n", [X]),
            io:format("    ===> Slave has ~p/~p responses~n", [CurrentResponses, ExpectedResponses]),
      ackLoop(ExpectedResponses, CurrentResponses, SeqNum, ExpectedReply)
   after getTimeout() -> % Phase 2 is not meant to timeout
      case debug() of debug -> io:format("~n~n     Controller timed out in 2nd phase of 2PC~n"); _ -> ignore end,
      ackLoop(ExpectedResponses, CurrentResponses, SeqNum, ExpectedReply)
   end.


coordinateFirstPhase(RoutingTable, SeqNum) ->
   Hops = [Hop || [{Dest, Hop}] <- ets:match(RoutingTable, '$1'), Dest =/= '$NoInEdges'],
   Neighbours = length(lists:usort(Hops)), % usort returns unique entries i.e. neighbours
   Phase1Result = awaitPhase1Responses(Neighbours, 0, SeqNum),
   case Phase1Result of
      X when X =:= phase1Abort; X =:= phase1Commit -> ignore;
      % match and catch unexpected messages
      X -> case debug() of debug -> io:format("2PhaseCommit Coordinator received odd message -> ~p~n", [X]); _ -> ignore end
   end,
   Phase1Result.


awaitPhase1Responses(ExpectedResponses, CurrentResponses, SeqNum) ->
   receive
      {'2pcPhase1', canCommit, CommitSeq, _} ->
         case CommitSeq of 
            SeqNum ->
               case CurrentResponses + 1 of
                  ExpectedResponses ->
                     phase1Commit;
                  _ -> % still waiting on responses
                     awaitPhase1Responses(ExpectedResponses, CurrentResponses + 1, SeqNum)
               end;
            _ -> % not this sequence, swallow it
               awaitPhase1Responses(ExpectedResponses, CurrentResponses, SeqNum)
         end;
      {'2pcPhase1', mustAbort, CommitSeq, _} ->
         case CommitSeq of
            SeqNum ->
               phase1Abort;
            _ -> % not this sequence, swallow it
               awaitPhase1Responses(ExpectedResponses, CurrentResponses, SeqNum)
         end;
      {control, From, _, SeqNum, _} -> % Control for this sequence has been sent in a cycle, send back "yes", so whoever sent it chooses yes/no correctly... We can still abort later
         From ! {'2pcPhase1', canCommit, SeqNum, self()},
         awaitPhase1Responses(ExpectedResponses, CurrentResponses, SeqNum);
      {control, From, _, SeqNum2, _} -> % Control is for a different sequence... During this sequence... Abort both!!!
         From ! {'2pcPhase1', mustAbort, SeqNum2, self()},
         phase1Abort
   after getTimeout() -> phase1Abort
   end.


coordinateSecondPhase(RoutingTable, SeqNum, ExpectedReply) ->
   Hops = [Hop || [{Dest, Hop}] <- ets:match(RoutingTable, '$1'), Dest =/= '$NoInEdges'],
   Neighbours = length(lists:usort(Hops)), % usort returns unique entries i.e. neighbours
   Phase2Result = awaitPhase2Responses(Neighbours, 0, SeqNum, ExpectedReply),
   case Phase2Result of
      X when X =:= timeout; X =:= phase2Good -> ignore;
      % match and catch unexpected messages
      X -> case debug() of debug -> io:format("2PhaseCommit Coordinator received odd message -> ~p~n", [X]); _ -> ignore end
   end,
   Phase2Result.


awaitPhase2Responses(ExpectedResponses, CurrentResponses, SeqNum, ExpectedReply) ->
   receive
      {control, From, _, SeqNum, _} -> % if we're really quick we can 
         io:format("Controller bad... very bad ~p~n", [From]);
      {control, From, _, SeqNum2, _} -> % if we're really quick we can 
         From ! {'2pcPhase1', mustAbort, SeqNum2, self()};
      {'2pcPhase1', mustAbort, _, From} -> % Matched for concurrent termination
         From ! {'2pcPhase2', confirmAbort, SeqNum, self()},
         awaitPhase2Responses(ExpectedResponses, CurrentResponses, SeqNum, ExpectedReply);
      % We're in phase 2 so fewer checks are needed
      {'2pcPhase2', commitChange, _, From} ->
         From ! {'2pcPhase2', confirmCommit, SeqNum, self()},
         awaitPhase2Responses(ExpectedResponses, CurrentResponses, SeqNum, ExpectedReply);
      {'2pcPhase2', abortChange, _, From} ->
         From ! {'2pcPhase2', confirmAbort, SeqNum, self()},
         awaitPhase2Responses(ExpectedResponses, CurrentResponses, SeqNum, ExpectedReply);
      {'2pcPhase2', Reply, SeqNum, _} ->
         case Reply == ExpectedReply of
            true -> ignore;  % as expected
            false -> case debug() of debug -> io:format("2nd phase received reply ~p but expected => ~p~n", [Reply, ExpectedReply]); _ -> ignore end
         end,
         case CurrentResponses + 1 of
            ExpectedResponses ->
               phase2Good;
            _ -> % still waiting on responses
               awaitPhase2Responses(ExpectedResponses, CurrentResponses + 1, SeqNum, ExpectedReply)
         end;
      {'2pcPhase2', _, CommitSeq, _} -> % not this sequence
         case debug() of debug -> io:format("Reply for sequence ~p but expected seq ~p~n", [CommitSeq, SeqNum]); _ -> ignore end,
         awaitPhase2Responses(ExpectedResponses, CurrentResponses, SeqNum, ExpectedReply);
      X -> io:format("    ===> Controller received message ~p~n", [X]),
      io:format("    ===> Slave has ~p/~p responses~n", [CurrentResponses, ExpectedResponses]),
         awaitPhase2Responses(ExpectedResponses, CurrentResponses, SeqNum, ExpectedReply)
   after getTimeout() ->  % Phase 2 is not meant to timeout
      case debug() of debug -> io:format("~n~n     Controller timed out in 2nd phase of 2PC~n"); _ -> ignore end,
      awaitPhase2Responses(ExpectedResponses, CurrentResponses, SeqNum, ExpectedReply)
   end.

%c(control, debug_info). c(router, debug_info). c(networkTest). c(controlTest). c(extendTest). c(modifyTest). c(concurrentTest, debug_info). debugger:start().

getTimeout() -> 500000.
% % % % %
% End Helpers
% % % % %
