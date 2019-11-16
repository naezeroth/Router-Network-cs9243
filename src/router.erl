-module(router).
-export([start/1]).


start (RouterName) ->
   spawn(fun() -> init(RouterName) end).


init(RouterName) ->
   io:format("Starting router: ~p with pid ~p~n", [RouterName, self()]),
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
      X ->
         io:format("Main loop swallowed message => ~p~n", [X])
   end.


% % % % %
% Handlers
% % % % %
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
   ets:insert(OtherTable, {executed, 0}),
   RoutingTable;
handleControl(RouterName, RoutingTable, OtherTable, Pid, Pid, SeqNum, ControlFun) ->
   % not init, coordinator of 2 phase commit
   [{_, Executed}] = ets:lookup(OtherTable, executed),
   Guard = lists:member(SeqNum, Executed),
   case Guard of
      true ->
         RoutingTable;  % Sequence number already executed
      false ->
         ets:insert(OtherTable, {executed, [SeqNum|Guard]}),
         TentativeRoutingTable = ets:new(undef,[private]),
         % ets:match returns a a list of lists or tuples, we flatten it to get a list of tuples
         % which we can insert in one go
         % Dump = lists:flatten(ets:match(RoutingTable, '$1')),
         Dump = ets:match(RoutingTable, '$1'),
         io:format("If this fails... We know why *** ~p ***~n", [Dump]),
         ets:insert(TentativeRoutingTable, Dump),
         % Broadcast the control to all nodes so that a) sequence nubmers are consistent and b) they expect the correct messages later
         broadcast(RoutingTable, {control, self(), Pid, SeqNum, ControlFun}),
         try ControlFun(RouterName, TentativeRoutingTable) of
            abort ->
               % ControlFun has failed, notify network to abort and then report abort
               broadcast(RoutingTable, {twoPhaseCommit, mustAbort, SeqNum}),
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
                  phase1Good ->
                     broadcast(RoutingTable, {twoPhaseCommit, canCommit, SeqNum}),
                     Phase2Response = coordinateSecondPhase(RoutingTable, SeqNum, confirmCommit),
                     case Phase2Response of
                        phase2Good -> ignore;
                        timeout -> io:foramt("Phase 2 timed out for coordinator~n")
                     end,
                     ets:delete(RoutingTable),
                     Pid ! {commited, self(), SeqNum},
                     TentativeRoutingTable;
                  X when X =:= timeout; X =:= phase1abort ->
                     broadcast(RoutingTable, {twoPhaseCommit, mustAbort, SeqNum}),
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
         end
   end;
handleControl(RouterName, RoutingTable, OtherTable, From, Pid, SeqNum, ControlFun) ->
   % not init, slave of 2 phase commit
   [{_, Executed}] = ets:lookup(OtherTable, executed),
   Guard = lists:member(SeqNum, Executed),
   case Guard of
      true ->
         RoutingTable;  % Sequence number already executed
      false ->
         ets:insert(OtherTable, {executed, [SeqNum|Guard]}),
         TentativeRoutingTable = ets:new(undef,[private]),
         % ets:match returns a a list of lists or tuples, we flatten it to get a list of tuples
         % which we can insert in one go
         % Dump = lists:flatten(ets:match(RoutingTable, '$1')),
         Dump = ets:match(RoutingTable, '$1'),
         io:format("If this fails... We know why *** ~p ***~n", [Dump]),
         ets:insert(TentativeRoutingTable, Dump),
         % Broadcast the control to all nodes so that a) sequence nubmers are consistent and b) they expect the correct messages later
         broadcast(RoutingTable, {control, self(), Pid, SeqNum, ControlFun, slave}),
         try ControlFun(RouterName, TentativeRoutingTable) of
            abort ->
               % ControlFun has failed somewhere so backpropagate abort, we rely on the coordinator to signal an abort to everyone
               From ! {twoPhaseCommit, ohGodNoNoNo, SeqNum},
               _ = participateFirstPhaseNo(RoutingTable, SeqNum, From),
               ets:delete(TentativeRoutingTable),
               RoutingTable;
            Children ->  % Will also capture empty list, in which case foreach does nothing later
               Phase1Response = participateFirstPhaseYes(RoutingTable, SeqNum, From),
               case Phase1Response of
                  phase1Good ->
                     ets:delete(RoutingTable),
                     From ! {commited, self(), SeqNum},
                     TentativeRoutingTable;
                  X when X =:= timeout; X =:= phase1abort ->
                     lists:foreach(fun(C) -> exit(C, "commit aborted") end, Children),
                     ets:delete(TentativeRoutingTable),
                     From ! {abort, self(), SeqNum},
                     RoutingTable;
                  _ ->
                     io:format("~n~nVery NOK~n"),
                     io:format("Phase 1 response is ~p~n", [Phase1Response]),
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
         end
   end.


handleDump(RoutingTable, From) ->
   io:format("Taking a dump~n"),
   Dump = ets:match(RoutingTable, '$1'),
   From ! {table, self(), Dump}.


handleStop(RouterName, RoutingTable, OtherTable) ->
   ets:delete(RoutingTable),
   ets:delete(OtherTable),
   io:format("Stopping router: ~p~n", [RouterName]).
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


participateFirstPhaseYes(RoutingTable, SeqNum, From) ->
   receive
      {twoPhaseCommit, yepSoundsGood, CommitSeq} ->
         case CommitSeq of
            SeqNum ->
               participateFirstPhaseYes(RoutingTable, SeqNum, From);
            _ -> % not this sequence
               nok
         end;
      {twoPhaseCommit, ohGodNoNoNo, CommitSeq} ->
         CommitSeq
   after
      5000 -> timeout
   end.


participateFirstPhaseNo(RoutingTable, SeqNum, From) ->
   NetworkSize = length(ets:match(RoutingTable, '$1')) - 1, % filter $NoInEdges from count
   Phase1Result = awaitPhase1Responses(NetworkSize, 0, SeqNum),
   case Phase1Result of
      X when X =:= timeout; X =:= phase1abort; X =:= phase1Good -> ignore;
      % match and catch unexpected messages
      _ -> io:format("2PhaseCommit Coordinator received odd message -> ~p~n", [Phase1Result])
   end,
   Phase1Result.


coordinateFirstPhase(RoutingTable, SeqNum) ->
   NetworkSize = length(ets:match(RoutingTable, '$1')) - 1, % filter $NoInEdges from count
   Phase1Result = awaitPhase1Responses(NetworkSize, 0, SeqNum),
   case Phase1Result of
      X when X =:= timeout; X =:= phase1abort; X =:= phase1Good -> ignore;
      % match and catch unexpected messages
      X -> io:format("2PhaseCommit Coordinator received odd message -> ~p~n", [X])
   end,
   Phase1Result.


awaitPhase1Responses(ExpectedResponses, CurrentResponses, SeqNum) ->
   receive
      {twoPhaseCommit, yepSoundsGood, CommitSeq} ->
         case CommitSeq of 
            SeqNum ->
               case CurrentResponses + 1 of
                  ExpectedResponses ->
                     phase1Good;
                  _ -> % still waiting on responses
                     awaitPhase1Responses(ExpectedResponses, CurrentResponses + 1, SeqNum)
               end;
            _ -> % not this sequence
               awaitPhase1Responses(ExpectedResponses, CurrentResponses, SeqNum)
         end;
      {twoPhaseCommit, ohGodNoNoNo, CommitSeq} ->
         case CommitSeq of
            SeqNum ->
               phase1abort;
            _ -> % not this sequence
               awaitPhase1Responses(ExpectedResponses, CurrentResponses, SeqNum)
         end
   after 5000 -> timeout
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
      {twoPhaseCommit, Reply, CommitSeq} ->
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
   after 5000 -> timeout
   end.
% % % % %
% End Helpers
% % % % %