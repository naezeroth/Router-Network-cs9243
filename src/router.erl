-module(router).
-export([start/1]).

debug() -> nope. % 'debug' for debug mode, else anything

getTimeout() -> 5000.

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
         NewRoutingTable = handleControl(RouterName, RoutingTable, OtherTable, From, Pid, SeqNum, ControlFun),
         loop(RouterName, NewRoutingTable, OtherTable);
      {dump, From} ->
         handleDump(RoutingTable, From),
         loop(RouterName, RoutingTable, OtherTable);
      stop ->
         handleStop(RouterName, RoutingTable, OtherTable);
      {'2pcPhase2', _, _, _} -> % Swallow up any extra 2pc messages (simultaneous control)
         loop(RouterName, RoutingTable, OtherTable);
      {'2pcPhase1', _, _, _} -> % Swallow up any extra 2pc messages (concurrent abort)
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
   ets:delete(OtherTable),
   clearMail(),
   exit(normal).


clearMail() ->
   receive _ -> clearMail()
   after 0 -> ok
   end.


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


handleControl(RouterName, RoutingTable, OtherTable, _, Pid, 0, ControlFun) ->
   case ControlFun(RouterName, RoutingTable) of
      abort -> % If we abort here things are very very bad....
         Pid ! {abort, self(), 0},
         exit(abort);
      _ ->  % No Children to abort in init
         Pid ! {committed, self(), 0},
         ets:insert(OtherTable, {executed, [0]}),
         RoutingTable
   end;
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
         start2PC(RouterName, RoutingTable, TentativeRoutingTable, OtherTable, From, Pid, SeqNum, ControlFun)
   end.


start2PC(RouterName, RoutingTable, TentativeRoutingTable, OtherTable, Pid, Pid, SeqNum, ControlFun) ->
   % coordinator of 2 phase commit
   % Broadcast the control to all nodes so that a) sequence nubmers are consistent and b) they expect the correct messages later
   broadcast(RoutingTable, {control, self(), Pid, SeqNum, ControlFun}),
   case ControlFun(RouterName, TentativeRoutingTable) of
      abort ->
         % We already know we're going to abort, but first we coordinate phase 1 so that everyone is on the same page...Efficiency third
         case debug() of debug -> io:format("~n~n      ~p: abort from ControlFun~n~n", [RouterName]); _ -> ignore end,
         coordinatePhase1(RoutingTable, OtherTable, SeqNum),
         broadcast(RoutingTable, {'2pcPhase2', abortChange, SeqNum, self()}),
         % coordinatePhase2(RoutingTable, SeqNum, confirmAbort),
         Pid ! {abort, self(), SeqNum},
         ets:delete(TentativeRoutingTable),
         RoutingTable;
      Children ->  % Will also capture empty list, in which case foreach does nothing later
         Phase1Result = coordinatePhase1(RoutingTable, OtherTable, SeqNum),
         case Phase1Result of
            phase1Commit ->  case debug() of debug -> io:format("~n~n      ~p: Signaling to network commitChange~n~n", [RouterName]); _ -> ignore end,
               broadcast(RoutingTable, {'2pcPhase2', commitChange, SeqNum, self()}),
               % coordinatePhase2(RoutingTable, SeqNum, confirmCommit),
               Pid ! {committed, self(), SeqNum},
               ets:delete(RoutingTable),
               TentativeRoutingTable;
            _ -> case debug() of debug -> io:format("~n~n      ~p: Signalling network abortChange~n~n", [RouterName]); _ -> ignore end,
               broadcast(RoutingTable, {'2pcPhase2', abortChange, SeqNum, self()}),
               % coordinatePhase2(RoutingTable, SeqNum, confirmAbort),
               lists:foreach(fun(C) -> exit(C, "commit aborted") end, Children),
               Pid ! {abort, self(), SeqNum},
               ets:delete(TentativeRoutingTable),
               RoutingTable
         end
   end;
start2PC(RouterName, RoutingTable, TentativeRoutingTable, OtherTable, From, Pid, SeqNum, ControlFun) ->
   % slave of 2 phase commit
   % Broadcast the control to all nodes so that a) sequence nubmers are consistent and b) they expect the correct messages later
   broadcast(RoutingTable, {control, self(), Pid, SeqNum, ControlFun}),
   case ControlFun(RouterName, TentativeRoutingTable) of % ControlFun should catch it's own errors and return abort safely
      abort ->
         % ControlFun has failed somewhere so backpropagate abort, we rely on the coordinator to signal an abort to everyone
         case debug() of debug -> io:format("~n~n      ~p: abort from ControlFun~n~n", [RouterName]); _ -> ignore end,
         Phase1Result = participatePhase1(RoutingTable, OtherTable, SeqNum, mustAbort),
         case Phase1Result of canCommit -> case debug() of debug -> io:format("~n~n   Router ~p asked to commit but signaled abort~n", [RouterName]); _ -> ignore end; _ -> ignore end,
         From ! {'2pcPhase1', Phase1Result, SeqNum, self()},
         Phase2Response = participatePhase2(SeqNum, true),
         case Phase2Response of commitChange -> case debug() of debug -> io:format("~n~n   Router ~p asked to commit but signaled abort~n", [RouterName]); _ -> ignore end; _ -> ignore end,
         broadcast(RoutingTable, {'2pcPhase2', abortChange, SeqNum, self()}),
         ets:delete(TentativeRoutingTable),
         RoutingTable;
      Children ->  % Will also capture empty list, in which case foreach does nothing later
         Phase1Result = participatePhase1(RoutingTable, OtherTable, SeqNum, canCommit),
         From ! {'2pcPhase1', Phase1Result, SeqNum, self()},
         Phase2Response = participatePhase2(SeqNum, false),
         broadcast(RoutingTable, {'2pcPhase2', Phase2Response, SeqNum, self()}),
         case Phase2Response of
            commitChange -> case debug() of debug -> io:format("~n~n      ~p: Network signalled commitChange~n~n", [RouterName]); _ -> ignore end,
               ets:delete(RoutingTable),
               TentativeRoutingTable;
            abortChange -> case debug() of debug -> io:format("~n~n      ~p: Network signalled abortChange~n~n", [RouterName]); _ -> ignore end,
               lists:foreach(fun(C) -> exit(C, "commit aborted") end, Children),
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


participatePhase1(RoutingTable, OtherTable, SeqNum, MyResponse) ->
   Hops = [Hop || [{Dest, Hop}] <- ets:match(RoutingTable, '$1'), Dest =/= '$NoInEdges'],
   Neighbours = length(lists:usort(Hops)), % usort returns unique entries i.e. neighbours
   Phase1Result = phase1MessageLoop(OtherTable, Neighbours, 0, SeqNum, MyResponse),
   case Phase1Result of
      X when X =:= mustAbort; X =:= canCommit -> ignore;
      % match and catch unexpected messages
      X -> case debug() of debug -> io:format("Phase 1 participant received odd message -> ~p~n", [X]); _ -> ignore end
   end,
   Phase1Result.


phase1MessageLoop(OtherTable, ExpectedResponses, CurrentResponses, SeqNum, MyResponse) ->
   receive
      {'2pcPhase1', canCommit, SeqNum, _} -> % Phase 1 back prop message
         case CurrentResponses + 1 of
            ExpectedResponses ->
               MyResponse;
            _ -> % Keep counting
               phase1MessageLoop(OtherTable, ExpectedResponses, CurrentResponses + 1, SeqNum, MyResponse)
         end;
      {'2pcPhase1', mustAbort, SeqNum, _} -> % Phase 1 back prop message
         case CurrentResponses + 1 of
            ExpectedResponses ->
               mustAbort;
            _ ->  % Change response to can't commit (even if we can) since a neighbour can't commit
               phase1MessageLoop(OtherTable, ExpectedResponses, CurrentResponses + 1, SeqNum, mustAbort)
         end;
      {control, From, _, SeqNum, _} ->
         From ! {'2pcPhase1', canCommit, SeqNum, self()},
         phase1MessageLoop(OtherTable, ExpectedResponses, CurrentResponses, SeqNum, MyResponse);

      {'2pcPhase1', _, SeqNum2, _} -> % other sequence
         [{_, Executed}] = ets:lookup(OtherTable, executed),
         Guard = lists:member(SeqNum2, Executed), % Check if in our list
         if Guard /= true -> ets:insert(OtherTable, {executed, [SeqNum|Executed]}); true -> ignore end,
         mustAbort;
      {control, Pid, Pid, SeqNum3, ControlFun} -> % Shouldn't normally happen but was observed when debugging
         self() ! {control, Pid, Pid, SeqNum3, ControlFun},
         phase1MessageLoop(OtherTable, ExpectedResponses, CurrentResponses, SeqNum, MyResponse);
      {control, From, _, SeqNum2, _} -> % Control is for a different sequence... During this sequence... Abort both!!!
         [{_, Executed}] = ets:lookup(OtherTable, executed),
         Guard = lists:member(SeqNum2, Executed), % Check if in our list
         if Guard /= true -> ets:insert(OtherTable, {executed, [SeqNum|Executed]}); true -> ignore end,
         From ! {'2pcPhase1', mustAbort, SeqNum2, self()}, % Abort the other sequence
         mustAbort
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

coordinatePhase1(RoutingTable, OtherTable, SeqNum) ->
   Hops = [Hop || [{Dest, Hop}] <- ets:match(RoutingTable, '$1'), Dest =/= '$NoInEdges'],
   Neighbours = length(lists:usort(Hops)), % usort returns unique entries i.e. neighbours
   Phase1Result = awaitPhase1Responses(OtherTable, Neighbours, 0, SeqNum),
   case Phase1Result of
      X when X =:= phase1Abort; X =:= phase1Commit -> ignore;
      % match and catch unexpected messages
      X -> case debug() of debug -> io:format("2PhaseCommit Coordinator received odd message -> ~p~n", [X]); _ -> ignore end
   end,
   Phase1Result.


awaitPhase1Responses(OtherTable, ExpectedResponses, CurrentResponses, SeqNum) ->
   receive
      {'2pcPhase1', canCommit, CommitSeq, _} ->
         case CommitSeq of 
            SeqNum ->
               case CurrentResponses + 1 of
                  ExpectedResponses ->
                     phase1Commit;
                  _ -> % still waiting on responses
                     awaitPhase1Responses(OtherTable, ExpectedResponses, CurrentResponses + 1, SeqNum)
               end;
            _ -> % not this sequence, swallow it
               awaitPhase1Responses(OtherTable, ExpectedResponses, CurrentResponses, SeqNum)
         end;
      {'2pcPhase1', mustAbort, CommitSeq, _} ->
         case CommitSeq of
            SeqNum ->
               phase1Abort;
            _ -> % not this sequence, swallow it
               awaitPhase1Responses(OtherTable, ExpectedResponses, CurrentResponses, SeqNum)
         end;
      {control, Pid, Pid, SeqNum3, ControlFun} -> % Shouldn't normally happen but was observed when debugging
         self() ! {control, Pid, Pid, SeqNum3, ControlFun},
         awaitPhase1Responses(OtherTable, ExpectedResponses, CurrentResponses, SeqNum);
      {control, From, _, SeqNum, _} -> % Control for this sequence has been sent in a cycle, send back "yes", so whoever sent it chooses yes/no correctly... We can still abort later
         From ! {'2pcPhase1', canCommit, SeqNum, self()},
         awaitPhase1Responses(OtherTable, ExpectedResponses, CurrentResponses, SeqNum);
      {control, From, _, SeqNum2, _} -> % Control is for a different sequence... During this sequence... Abort both!!!
         [{_, Executed}] = ets:lookup(OtherTable, executed),
         Guard = lists:member(SeqNum2, Executed), % Check if in our list
         if Guard /= true -> ets:insert(OtherTable, {executed, [SeqNum|Executed]}); true -> ignore end,
         From ! {'2pcPhase1', mustAbort, SeqNum2, self()},
         phase1Abort
   after getTimeout() -> phase1Abort
   end.
% % % % %
% End Helpers
% % % % %
