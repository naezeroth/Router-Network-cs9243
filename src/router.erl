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
      {control, From, Pid, SeqNum, ControlFun} -> % From == Pid => from controller
         % Pattern matching calls correct function partial when From == Pid and when SeqNum == 0
         NewRoutingTable = handleControl(RouterName, RoutingTable, OtherTable, From, Pid, SeqNum, ControlFun),
         loop(RouterName, NewRoutingTable, OtherTable);
      {dump, From} ->
         handleDump(RoutingTable, From),
         loop(RouterName, RoutingTable, OtherTable);
      stop ->
         handleStop(RouterName, RoutingTable, OtherTable);
      X ->
         io:format("Unhandled message type in loop => ~p~n", [X])
   end.


% % % % %
% Handlers
% % % % %
handleMessage(RouterName, RoutingTable, Dest, _, Pid, Trace) ->
   NextTrace = [RouterName|Trace],
   case Dest == RouterName of
      true ->
         Pid ! {trace, self(), lists:reverse(NextTrace)};
      false ->
         Next = ets:lookup(RoutingTable, Dest),
         case Next /= [] of
            true ->
               [{_, Next_Pid}] = Next,
               Next_Pid ! {message, Dest, self(), Pid, NextTrace};
            false ->
               %% Bad things have happened...
               io:format("Router ~p has received a message to ~p it can't route~n", [RouterName, Dest])
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
         try ControlFun(RouterName, TentativeRoutingTable) of
            abort ->
               % ControlFun has failed somewhere so report abort to controller
               % as we are root no other router on the network should know about the control request
               Pid ! {abort, self(), SeqNum},
               ets:delete(TentativeRoutingTable),
               RoutingTable;
            Children ->  % Will also capture empty list, in which case foreach does nothing later
               broadcast2PhaseCommit(RoutingTable, {control, self(), Pid, SeqNum, ControlFun}),
               FirstPhaseResponse = coordinateFirstPhase(RoutingTable, SeqNum),
               case FirstPhaseResponse of
                  X when X =:= timeout; X =:= phase1abort ->
                     lists:foreach(fun(C) -> exit(C, "commit aborted") end, Children),
                     ets:delete(TentativeRoutingTable),
                     Pid ! {abort, self(), SeqNum},
                     RoutingTable;
                  phase1Good ->
                     ets:delete(RoutingTable),
                     Pid ! {commited, self(), SeqNum},
                     TentativeRoutingTable;
                  _ ->
                     io:format("~n~nVery NOK~n"),
                     io:format("Phase 1 response is ~p~n", [FirstPhaseResponse]),
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
         try ControlFun(RouterName, TentativeRoutingTable) of
            abort ->
               % ControlFun has failed somewhere so backpropagate abort
               From ! {twoPhaseCommit, ohGodNoNoNo, SeqNum},
               _ = participateFirstPhaseNo(RoutingTable, SeqNum),
               ets:delete(TentativeRoutingTable),
               RoutingTable;
            Children ->  % Will also capture empty list, in which case foreach does nothing later
               broadcast2PhaseCommit(RoutingTable, {control, self(), Pid, SeqNum, ControlFun, slave}),
               FirstPhaseResponse = participateFirstPhaseYes(RoutingTable, SeqNum),
               case FirstPhaseResponse of
                  X when X =:= timeout; X =:= phase1abort ->
                     lists:foreach(fun(C) -> exit(C, "commit aborted") end, Children),
                     ets:delete(TentativeRoutingTable),
                     From ! {abort, self(), SeqNum},
                     RoutingTable;
                  phase1Good ->
                     ets:delete(RoutingTable),
                     From ! {commited, self(), SeqNum},
                     TentativeRoutingTable;
                  _ ->
                     io:format("~n~nVery NOK~n"),
                     io:format("Phase 1 response is ~p~n", [FirstPhaseResponse]),
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
broadcast2PhaseCommit(RoutingTable, Msg) ->
   % We need to filter out the NoInEdges which is stored here... Luckily the atom is knowm :)
   Hops = [Hop || [{Dest, Hop}] <- ets:match(RoutingTable, '$1'), Dest /= '$NoInEdges'],
   lists:foreach(fun(Hop) -> Hop ! Msg end, Hops).


participateFirstPhaseNo(RoutingTable, SeqNum) ->
   NetworkSize = length(ets:match(RoutingTable, '$1')) - 1, % filter $NoInEdges from count
   Phase1Result = awaitPhase1Responses(NetworkSize, 0, SeqNum),
   case Phase1Result of
      phase1Good ->
         phase1Good;
      phase1abort ->
         phase1abort;
      timeout ->
         timeout;
      X ->
         io:format("2PhaseCommit Coordinator received odd message -> ~p~n", [X])
   end.


participateFirstPhaseYes(RoutingTable, SeqNum) ->
   receive
      {twoPhaseCommit, yepSoundsGood, CommitSeq} ->
         case CommitSeq of
            SeqNum ->
               participateFirstPhaseYes(RoutingTable, SeqNum);
            _ -> % not this sequence
               nok
         end;
      {twoPhaseCommit, ohGodNoNoNo, CommitSeq} ->
         nok
   after
      5000 ->
         timeout
   end.


coordinateFirstPhase(RoutingTable, SeqNum) ->
   NetworkSize = length(ets:match(RoutingTable, '$1')) - 1, % filter $NoInEdges from count
   Phase1Result = awaitPhase1Responses(NetworkSize, 0, SeqNum),
   case Phase1Result of
      phase1Good ->
         phase1Good;
      phase1abort ->
         phase1abort;
      timeout ->
         timeout;
      X ->
         io:format("2PhaseCommit Coordinator received odd message -> ~p~n", [X])
   end.


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
   after 5000 -> % From spec, don't change this
         timeout
   end.
% % % % %
% End Helpers
% % % % %