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
      {control, From, Pid, SeqNum, ControlFun} ->
         NewRoutingTable = handleControl(RouterName, RoutingTable, OtherTable, From, Pid, SeqNum, ControlFun, coordinator),
         loop(RouterName, NewRoutingTable, OtherTable);
      {control, From, Pid, SeqNum, ControlFun, slave} ->
         NewRoutingTable = handleControl(RouterName, RoutingTable, OtherTable, From, Pid, SeqNum, ControlFun, slave),
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
   if Dest == RouterName ->
      Pid ! {trace, self(), lists:reverse(NextTrace)};
   true ->
      Next = ets:lookup(RoutingTable, Dest),
      if Next /= [] ->
         [{_, Next_Pid}] = Next,
         Next_Pid ! {message, Dest, self(), Pid, NextTrace};
      true ->
         %% Bad things have happened...
         io:format("Router ~p has received a message to ~p it can't route~n", [RouterName, Dest])
      end
   end.


handleControl(RouterName, RoutingTable, OtherTable, _, _, 0, ControlFun, _) ->
   % init
   _ = ControlFun(RouterName, RoutingTable), % Initialization shouldn't have any reason to fail, or have created any new nodes
   ets:insert(OtherTable, {executed, 0}),
   RoutingTable;
handleControl(RouterName, RoutingTable, OtherTable, From, Pid, SeqNum, ControlFun, coordinator) ->
   % not init, coordinator of control
   [{_, Executed}] = ets:lookup(OtherTable, executed),
   Guard = lists:member(SeqNum, Executed),
   if Guard == true ->
         RoutingTable;  % Sequence number already executed
      true ->
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
               From ! {abort, self(), SeqNum}, % ControlFun has failed somewhere so report abort
               ets:delete(TentativeRoutingTable),
               RoutingTable;
            Children ->
               broadcast2PhaseCommit(RoutingTable, {control, self(), Pid, SeqNum, ControlFun, slave}),
               coordinate2PhaseCommit(RoutingTable, TentativeRoutingTable, SeqNum),
               % Loop as coordinator
               Children
         catch
            _:_ ->  % ... Should catch everything
               From ! {abort, self(), SeqNum},
               ets:delete(TentativeRoutingTable),
               RoutingTable
         end
   end;
handleControl(RouterName, RoutingTable, OtherTable, From, Pid, SeqNum, ControlFun, slave) ->
   % not init, slave of control
   ok.


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

coordinate2PhaseCommit(RoutingTable, TentativeRoutingTable, SeqNum) ->
   NetworkSize = length(ets:match(RoutingTable, '$1')) - 1, % filter $NoInEdges from count
   Phase1Result = awaitPhase1Responses(NetworkSize, 0, SeqNum),
   ok.

awaitPhase1Responses(ExpectedResponses, CurrentResponses, SeqNum) ->
   receive
      {twoPhaseCommit, yepSoundsGood, CommitSeq} ->
         if CommitSeq == SeqNum ->
            if CurrentResponses + 1 == ExpectedResponses ->
               phase1Good;
            true ->
               awaitPhase1Responses(ExpectedResponses, CurrentResponses + 1, SeqNum)
            end;
         true ->
               awaitPhase1Responses(ExpectedResponses, CurrentResponses, SeqNum)
         end;
      {twoPhaseCommit, ohGodNoNoNo, CommitSeq} ->
         if CommitSeq == SeqNum ->
            phase1Abort;
         true ->
            awaitPhase1Responses(ExpectedResponses, CurrentResponses, SeqNum)
         end
   after 5000 ->
         timeout
   end.
% % % % %
% End Helpers
% % % % %