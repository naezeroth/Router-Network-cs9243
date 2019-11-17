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
         NewRoutingTable = handleControl(RouterName, RoutingTable, OtherTable, From, Pid, SeqNum, ControlFun),
         loop(RouterName, NewRoutingTable, OtherTable);
      {dump, From} ->
         handleDump(RoutingTable, From),
         loop(RouterName, RoutingTable, OtherTable);
      stop ->
         handleStop(RouterName, RoutingTable, OtherTable);
      {'2pcPhase2', _, _} -> % Swallow up any extra 2pc messages
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
   broadcast(RoutingTable, {control, self(), Pid, SeqNum, ControlFun}),
   try ControlFun(RouterName, TentativeRoutingTable) of
      abort ->
         % We already know we're going to abort, but first we coordinate phase 1 so that everyone is on the same page...Efficiency third
         case debug() of debug -> io:format("~n~n      ~p: abort from ControlFun~n~n", [RouterName]); _ -> ignore end,
         _ = coordinateFirstPhase(RoutingTable, SeqNum),
         broadcast(RoutingTable, {'2pcPhase2', mustAbort, SeqNum}),
         Phase2Response = coordinateSecondPhase(RoutingTable, SeqNum, confirmAbort),
         case Phase2Response of
            phase2Good -> ignore;
            timeout -> case debug() of debug -> io:format("Phase 2 timed out for coordinator~n"); _ -> ignore end
         end,
         Pid ! {abort, self(), SeqNum},
         ets:delete(TentativeRoutingTable),
         RoutingTable;
      Children ->  % Will also capture empty list, in which case foreach does nothing later
         Phase1Response = coordinateFirstPhase(RoutingTable, SeqNum),
         case Phase1Response of
            phase1Commit ->  case debug() of debug -> io:format("~n~n      ~p: Signaling to network canCommit~n~n", [RouterName]); _ -> ignore end,
               broadcast(RoutingTable, {'2pcPhase2', canCommit, SeqNum}),
               Phase2Response = coordinateSecondPhase(RoutingTable, SeqNum, confirmCommit),
               case Phase2Response of
                  phase2Good -> ignore;
                  timeout -> case debug() of debug -> io:format("Phase 2 timed out for coordinator~n"); _ -> ignore end
               end,
               ets:delete(RoutingTable),
               Pid ! {committed, self(), SeqNum},
               TentativeRoutingTable;
            X when X =:= timeout; X =:= phase1Abort -> case debug() of debug -> io:format("~n~n      ~p: Signalling network mustAbort~n~n", [RouterName]); _ -> ignore end,
               broadcast(RoutingTable, {'2pcPhase2', mustAbort, SeqNum}),
               Phase2Response = coordinateSecondPhase(RoutingTable, SeqNum, confirmAbort),
               case Phase2Response of
                  phase2Good -> ignore;
                  timeout -> case debug() of debug -> io:format("Phase 2 timed out for coordinator~n"); _ -> ignore end
               end,
               lists:foreach(fun(C) -> exit(C, "commit aborted") end, Children),
               Pid ! {abort, self(), SeqNum},
               ets:delete(TentativeRoutingTable),
               RoutingTable
         end
   catch
      _:_ ->  % ... Should catch everything
         Pid ! {abort, self(), SeqNum},
         ets:delete(TentativeRoutingTable),
         RoutingTable
   end;
start2PC(RouterName, RoutingTable, TentativeRoutingTable, From, Pid, SeqNum, ControlFun) ->
   % slave of 2 phase commit
   % Broadcast the control to all nodes so that a) sequence nubmers are consistent and b) they expect the correct messages later
   broadcast(RoutingTable, {control, self(), Pid, SeqNum, ControlFun}),
   try ControlFun(RouterName, TentativeRoutingTable) of
      abort ->
         % ControlFun has failed somewhere so backpropagate abort, we rely on the coordinator to signal an abort to everyone
         case debug() of debug -> io:format("~n~n      ~p: abort from ControlFun~n~n", [RouterName]); _ -> ignore end,
         From ! {'2pcPhase1', ohGodNoNoNo, SeqNum}, % this is phase 1
         {Phase2Response, BackProp} = twoPhaseCommitLoop(RoutingTable, SeqNum, From, true, 0),
         case debug() of debug -> io:format("~n~n     ~p has BackProp count of ~p~n", [RouterName, BackProp]); _ -> ignore end,
         From ! {'2pcPhase2', confirmAbort, SeqNum},
         backpropAcks(From, BackProp, confirmAbort, SeqNum),
         case Phase2Response of
            canCommit -> case debug() of debug -> io:format("~n~n   Router ~p asked to commit but signaled abort~n", [RouterName]); _ -> ignore end;
            X when X =:= timeout; X =:= mustAbort -> ignore
         end,
         ets:delete(TentativeRoutingTable),
         RoutingTable;
      Children ->  % Will also capture empty list, in which case foreach does nothing later
         From ! {'2pcPhase1', yepSoundsGood, SeqNum}, % this is phase 1
         {Phase2Response, BackProp} = twoPhaseCommitLoop(RoutingTable, SeqNum, From, false, 0),
         case debug() of debug -> io:format("~n~n     ~p has BackProp count of ~p~n", [RouterName, BackProp]); _ -> ignore end,
         case Phase2Response of
   canCommit -> case debug() of debug -> io:format("~n~n      ~p: Network signalled canCommit~n~n", [RouterName]); _ -> ignore end,
               backpropAcks(From, BackProp, confirmCommit, SeqNum),
               ets:delete(RoutingTable),
               TentativeRoutingTable;
               X when X =:= timeout; X =:= mustAbort -> case debug() of debug -> io:format("~n~n      ~p: Network signalled mustAbort~n~n", [RouterName]); _ -> ignore end,
               lists:foreach(fun(C) -> exit(C, "commit aborted") end, Children),
               From ! {'2pcPhase2', confirmAbort, SeqNum},
               backpropAcks(From, BackProp, confirmAbort, SeqNum),
               ets:delete(TentativeRoutingTable),
               RoutingTable
         end
   catch
      _:_ ->  % ... Should catch everything
         From ! {confirmAbort, self(), SeqNum},
         ets:delete(TentativeRoutingTable),
         RoutingTable
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


backpropAcks(From, BackProp, Reply, SeqNum) ->
   case BackProp of
      0 -> From ! {'2pcPhase2', Reply, SeqNum}; % then send an ack for ourselves
      _ ->
         receive % Send an ack for every backprop we received in phase 1
            {'2pcPhase2', Reply, SeqNum} ->
               From ! {'2pcPhase2', Reply, SeqNum},
               backpropAcks(From, BackProp - 1, Reply, SeqNum)
         end
   end.


twoPhaseCommitLoop(RoutingTable, SeqNum, From, MustFail, BackProp) ->
   receive
      {'2pcPhase1', yepSoundsGood, CommitSeq} -> % Phase 1 back prop message
         case CommitSeq of
            SeqNum ->
               From ! {'2pcPhase1', yepSoundsGood, SeqNum},
               BackProp2 = BackProp + 1;
            _ -> BackProp2 = BackProp % different sequence, don't increment
         end,
         twoPhaseCommitLoop(RoutingTable, SeqNum, From, MustFail, BackProp2);
      {'2pcPhase1', ohGodNoNoNo, CommitSeq} -> % Phase 1 back prop message
         case CommitSeq of
            SeqNum ->
               From ! {'2pcPhase1', ohGodNoNoNo, SeqNum},
               BackProp2 = BackProp + 1;
            _ -> BackProp2 = BackProp % different sequence, don't increment
         end,
         twoPhaseCommitLoop(RoutingTable, SeqNum, From, true, BackProp2);
      {'2pcPhase2', canCommit, CommitSeq} -> % Phase 2 broadcast message
         case CommitSeq of
            SeqNum ->
               case MustFail of
            true -> case debug() of debug -> io:format("~n~n      Router received canCommit, but has sent of seen an ohGodNoNoNo~n~n"); _ -> ignore end;
                  _ -> ignored
               end,
               broadcast(RoutingTable, {'2pcPhase2', canCommit, SeqNum}),
               {canCommit, BackProp};
            _ ->
               twoPhaseCommitLoop(RoutingTable, SeqNum, From, MustFail, BackProp)
         end;
      {'2pcPhase2', mustAbort, CommitSeq} -> % Phase 2 broadcast message
         case CommitSeq of
            SeqNum ->
               broadcast(RoutingTable, {'2pcPhase2', mustAbort, SeqNum}),
               {mustAbort, BackProp};
            _ ->
               twoPhaseCommitLoop(RoutingTable, SeqNum, From, MustFail, BackProp)
         end;
         {control, From2, _, SeqNum2, _} ->
            case SeqNum2 of 
               SeqNum -> % Control for this sequence has been sent twice, this is fine, just swallow it
                  twoPhaseCommitLoop(RoutingTable, SeqNum, From, MustFail, BackProp);
               _ -> % Control is for a different sequence... During this sequence... Abort both!!!
                  From ! {'2pcPhase1', ohGodNoNoNo, SeqNum},
                  From2 ! {'2pcPhase1', ohGodNoNoNo, SeqNum2}
            end
   after getTimeout() -> {timeout, BackProp}
   end.


coordinateFirstPhase(RoutingTable, SeqNum) ->
   NetworkSize = length(ets:match(RoutingTable, '$1')) - 1, % filter $NoInEdges from count
   Phase1Result = awaitPhase1Responses(NetworkSize, 0, SeqNum),
   case Phase1Result of
      X when X =:= timeout; X =:= phase1Abort; X =:= phase1Commit -> ignore;
      % match and catch unexpected messages
      X -> case debug() of debug -> io:format("2PhaseCommit Coordinator received odd message -> ~p~n", [X]); _ -> ignore end
   end,
   Phase1Result.


awaitPhase1Responses(ExpectedResponses, CurrentResponses, SeqNum) ->
   receive
      {'2pcPhase1', yepSoundsGood, CommitSeq} ->
         case CommitSeq of 
            SeqNum ->
               case CurrentResponses + 1 of
                  ExpectedResponses ->
                     phase1Commit;
                  _ -> % still waiting on responses
                     awaitPhase1Responses(ExpectedResponses, CurrentResponses + 1, SeqNum)
               end;
            _ -> % not this sequence
               awaitPhase1Responses(ExpectedResponses, CurrentResponses, SeqNum)
         end;
      {'2pcPhase1', ohGodNoNoNo, CommitSeq} ->
         case CommitSeq of
            SeqNum ->
               phase1Abort;
            _ -> % not this sequence
               awaitPhase1Responses(ExpectedResponses, CurrentResponses, SeqNum)
         end;
      {control, From2, _, SeqNum2, _} ->
         case SeqNum2 of 
            SeqNum -> awaitPhase1Responses(ExpectedResponses, CurrentResponses, SeqNum); % Control for this sequence has been seen twice, this is fine, just swallow it
            _ -> % Control is for a different sequence... During this sequence... Abort both!!!
               From2 ! {'2pcPhase1', ohGodNoNoNo, SeqNum2},
               phase1Abort
         end
   after getTimeout() -> timeout
   end.


coordinateSecondPhase(RoutingTable, SeqNum, ExpectedReply) ->
   NetworkSize = length(ets:match(RoutingTable, '$1')) - 1, % filter $NoInEdges from count
   Phase2Result = awaitPhase2Responses(NetworkSize, 0, SeqNum, ExpectedReply),
   case Phase2Result of
      X when X =:= timeout; X =:= phase2Good -> ignore;
      % match and catch unexpected messages
   X -> case debug() of debug -> io:format("2PhaseCommit Coordinator received odd message -> ~p~n", [X]); _ -> ignore end
   end,
   Phase2Result.


awaitPhase2Responses(ExpectedResponses, CurrentResponses, SeqNum, ExpectedReply) ->
   receive
      % ignored
      {'2pcPhase2', 'canCommit', _} -> awaitPhase2Responses(ExpectedResponses, CurrentResponses, SeqNum, ExpectedReply);
      {'2pcPhase2', 'mustAbort', _} -> awaitPhase2Responses(ExpectedResponses, CurrentResponses, SeqNum, ExpectedReply);
      {'2pcPhase2', Reply, CommitSeq} ->
         case CommitSeq of 
            SeqNum ->
               case Reply =:= ExpectedReply of
                  true -> ignore;  % as expected
                  false -> case debug() of debug -> io:format("2nd phase received reply ~p but expected => ~p~n", [Reply, ExpectedReply]); _ -> ignore end
               end,
               case CurrentResponses + 1 of
                  ExpectedResponses ->
                     phase2Good;
                  _ -> % still waiting on responses
                     awaitPhase2Responses(ExpectedResponses, CurrentResponses + 1, SeqNum, ExpectedReply)
               end;
            _ -> % not this sequence
               case debug() of debug -> io:format("Reply for sequence ~p but expected seq ~p~n", [CommitSeq, SeqNum]); _ -> ignore end,
               awaitPhase2Responses(ExpectedResponses, CurrentResponses, SeqNum, ExpectedReply)
         end
   after getTimeout() ->  % Phase 2 is apparently not supposed to timeout
      case debug() of debug -> io:format("~n~n     Controller timed out in 2nd phase of 2PC~n"); _ -> ignore end,
      awaitPhase2Responses(ExpectedResponses, CurrentResponses, SeqNum, ExpectedReply)
   end.


getTimeout() -> 5000.
% % % % %
% End Helpers
% % % % %
