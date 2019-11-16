-module(router).
-export([start/1]).


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
         % Pattern matching calls correct function partial when From =:= Pid and when SeqNum =:= 0
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
         io:format("~p: Main loop swallowed message => ~p~n", [RouterName, X]),
         loop(RouterName, RoutingTable, OtherTable)
   end.


% % % % %
% Handlers
% % % % %
handleDump(RoutingTable, From) ->
   Dump = ets:match(RoutingTable, '$1'),
   From ! {table, self(), Dump}.


handleStop(RouterName, RoutingTable, OtherTable) ->
   ets:delete(RoutingTable),
   ets:delete(OtherTable),
   io:format("Stopping router: ~p~n", [RouterName]).


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
               io:format("Router ~p has received a message to ~p it can't route~n", [RouterName, Dest]);
            _ ->
               [{_, Next_Pid}] = Next,
               Next_Pid ! {message, Dest, self(), Pid, NextTrace}
         end
   end.


handleControl(RouterName, RoutingTable, OtherTable, _, _, 0, ControlFun) ->
   % init
   _ = ControlFun(RouterName, RoutingTable), % Initialization shouldn't have any reason to fail, or have created any new nodes
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
         ets:insert(OtherTable, {executed, [SeqNum|Guard]}),
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
         io:format("~n~n      ~p: abort from ControlFun~n~n", [RouterName]),
         _ = coordinateFirstPhase(RoutingTable, SeqNum),
         broadcast(RoutingTable, {'2pcPhase2', mustAbort, SeqNum}),
         Phase2Response = coordinateSecondPhase(RoutingTable, SeqNum, confirmAbort),
         case Phase2Response of
            phase2Good -> ignore;
            timeout -> io:foramt("Phase 2 timed out for coordinator~n")
         end,
         Pid ! {abort, self(), SeqNum},
         ets:delete(TentativeRoutingTable),
         RoutingTable;
      Children ->  % Will also capture empty list, in which case foreach does nothing later
         Phase1Response = coordinateFirstPhase(RoutingTable, SeqNum),
         case Phase1Response of
            phase1Commit ->  io:format("~n~n      ~p: Signaling to network canCommit~n~n", [RouterName]),
               broadcast(RoutingTable, {'2pcPhase2', canCommit, SeqNum}),
               Phase2Response = coordinateSecondPhase(RoutingTable, SeqNum, confirmCommit),
               case Phase2Response of
                  phase2Good -> ignore;
                  timeout -> io:foramt("Phase 2 timed out for coordinator~n")
               end,
               ets:delete(RoutingTable),
               Pid ! {committed, self(), SeqNum},
               TentativeRoutingTable;
            X when X =:= timeout; X =:= phase1Abort -> io:format("~n~n      ~p: Signalling network mustAbort~n~n", [RouterName]),
               broadcast(RoutingTable, {'2pcPhase2', mustAbort, SeqNum}),
               Phase2Response = coordinateSecondPhase(RoutingTable, SeqNum, confirmAbort),
               case Phase2Response of
                  phase2Good -> ignore;
                  timeout -> io:foramt("Phase 2 timed out for coordinator~n")
               end,
               lists:foreach(fun(C) -> exit(C, "commit aborted") end, Children),
               ets:delete(TentativeRoutingTable),
               Pid ! {abort, self(), SeqNum},
               RoutingTable;
            _ ->
               io:format("~n~nVery NOK~n"),
               io:format("Phase 1 response is ~p~n", [Phase1Response]),
               lists:foreach(fun(C) -> exit(C, "commit aborted") end, Children),
               ets:delete(TentativeRoutingTable),
               Pid ! {abort, self(), SeqNum},
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
         io:format("~n~n      ~p: abort from ControlFun~n~n", [RouterName]),
         From ! {'2pcPhase1', ohGodNoNoNo, SeqNum}, % this is phase 1
         Phase2Response = twoPhaseCommitLoop(RoutingTable, SeqNum, From, true),
         case Phase2Response of
            canCommit -> io:foramt("~n~n   Router ~p asked to commit but signaled abort~n", [RouterName]);
            X when X =:= timeout; X =:= mustAbort -> ignore
         end,
         ets:delete(TentativeRoutingTable),
         RoutingTable;
      Children ->  % Will also capture empty list, in which case foreach does nothing later
         From ! {'2pcPhase1', yepSoundsGood, SeqNum}, % this is phase 1
         Phase2Response = twoPhaseCommitLoop(RoutingTable, SeqNum, From, false),
         case Phase2Response of
            canCommit -> io:format("~n~n~p: Network signalled canCommit~n~n", [RouterName]),
               From ! {'2pcPhase2', confirmCommit, SeqNum},
               ets:delete(RoutingTable),
               TentativeRoutingTable;
            X when X =:= timeout; X =:= mustAbort -> io:format("~n~n~p: Network signalled mustAbort~n~n", [RouterName]),
               lists:foreach(fun(C) -> exit(C, "commit aborted") end, Children),
               ets:delete(TentativeRoutingTable),
               From ! {abort, self(), SeqNum},
               RoutingTable;
            _ ->
               io:format("~n~nVery NOK~n"),
               io:format("Phase 1 response is ~p~n", [Phase2Response]),
               lists:foreach(fun(C) -> exit(C, "commit aborted") end, Children),
               ets:delete(TentativeRoutingTable),
               From ! {abort, self(), SeqNum},
               RoutingTable
         end
   catch
      _:_ ->  % ... Should catch everything
         From ! {abort, self(), SeqNum},
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
   lists:foreach(fun(Hop) -> Hop ! Msg end, Hops).


twoPhaseCommitLoop(RoutingTable, SeqNum, From, MustFail) ->
   receive
      {'2pcPhase1', yepSoundsGood, CommitSeq} -> % Phase 1 back prop message
         case CommitSeq of
            SeqNum ->
               From ! {'2pcPhase1', yepSoundsGood, SeqNum};
            _ -> ignored
         end,
         twoPhaseCommitLoop(RoutingTable, SeqNum, From, MustFail);
      {'2pcPhase1', ohGodNoNoNo, CommitSeq} -> % Phase 1 back prop message
         case CommitSeq of
            SeqNum ->
               From ! {'2pcPhase1', ohGodNoNoNo, SeqNum};
            _ -> ignored
         end,
         twoPhaseCommitLoop(RoutingTable, SeqNum, From, true);
      {'2pcPhase2', canCommit, CommitSeq} -> % Phase 2 broadcast message
         case CommitSeq of
            SeqNum ->
               case MustFail of
                  true -> io:format("~n~n      Router received canCommit, but has sent of seen an ohGodNoNoNo~n~n");
                  _ -> ignored
               end,
               broadcast(RoutingTable, {'2pcPhase2', canCommit, SeqNum}),
               canCommit;
            _ ->
               twoPhaseCommitLoop(RoutingTable, SeqNum, From, MustFail)
         end;
      {'2pcPhase2', mustAbort, CommitSeq} -> % Phase 2 broadcast message
         case CommitSeq of
            SeqNum ->
               broadcast(RoutingTable, {'2pcPhase2', mustAbort, SeqNum}),
               mustAbort;
            _ ->
               twoPhaseCommitLoop(RoutingTable, SeqNum, From, MustFail)
         end;
      {control, _, _, CommitSeq, _} ->
         case CommitSeq of
               SeqNum -> ignored;
               _ -> io:format("Received SeqNum ~p while waiting for ~p~n", [CommitSeq, SeqNum])
            end,
            twoPhaseCommitLoop(RoutingTable, SeqNum, From, MustFail);
      X ->
         io:format("~n~n   2pc participant received unhandled message ~p~n", [X]),
         twoPhaseCommitLoop(RoutingTable, SeqNum, From, MustFail)
   after getTimeout() -> timeout
   end.


coordinateFirstPhase(RoutingTable, SeqNum) ->
   NetworkSize = length(ets:match(RoutingTable, '$1')) - 1, % filter $NoInEdges from count
   Phase1Result = awaitPhase1Responses(NetworkSize, 0, SeqNum),
   case Phase1Result of
      X when X =:= timeout; X =:= phase1Abort; X =:= phase1Commit -> ignore;
      % match and catch unexpected messages
      X -> io:format("2PhaseCommit Coordinator received odd message -> ~p~n", [X])
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
         end
   after getTimeout() -> timeout
   end.


coordinateSecondPhase(RoutingTable, SeqNum, ExpectedReply) ->
   NetworkSize = length(ets:match(RoutingTable, '$1')) - 1, % filter $NoInEdges from count
   Phase2Result = awaitPhase2Responses(NetworkSize, 0, SeqNum, ExpectedReply),
   case Phase2Result of
      X when X =:= timeout; X =:= phase2Good -> ignore;
      % match and catch unexpected messages
      _ -> io:format("2PhaseCommit Coordinator received odd message -> ~p~n", [Phase2Result])
   end,
   Phase2Result.


awaitPhase2Responses(ExpectedResponses, CurrentResponses, SeqNum, ExpectedReply) ->
   receive
      % ignored
      {'2pcPhase2', 'canCommit', _} -> awaitPhase2Responses(ExpectedResponses, CurrentResponses + 1, SeqNum, ExpectedReply);
      {'2pcPhase2', 'mustAbort', _} -> awaitPhase2Responses(ExpectedResponses, CurrentResponses + 1, SeqNum, ExpectedReply);
      {'2pcPhase2', Reply, CommitSeq} ->
         case CommitSeq of 
            SeqNum ->
               case Reply =:= ExpectedReply of
                  true -> ignore;  % as expected
                  false -> io:format("2nd phase received reply ~p but expected => ~p~n", [Reply, ExpectedReply])
               end,
               case CurrentResponses + 1 of
                  ExpectedResponses ->
                     phase2Good;
                  _ -> % still waiting on responses
                     awaitPhase2Responses(ExpectedResponses, CurrentResponses + 1, SeqNum, ExpectedReply)
               end;
            _ -> % not this sequence
               io:format("Reply for sequence ~p but expected seq ~p~n", [CommitSeq, SeqNum]),
               awaitPhase2Responses(ExpectedResponses, CurrentResponses, SeqNum, ExpectedReply)
         end
   after getTimeout() -> timeout
   end.

getTimeout() -> 500000.
% % % % %
% End Helpers
% % % % %
