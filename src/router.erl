-module(router).
-export([start/1]).

start (RouterName) ->
   spawn(fun() -> init(RouterName) end).


init(RouterName) ->
   io:format("Starting router: ~p with pid ~p~n", [RouterName, self()]),
   RoutingTable = ets:new(table, [private]),
   ets:insert(RoutingTable, {'$NoInEdges', 0}),
   loop(RouterName, RoutingTable).


loop (RouterName, RoutingTable) ->
   receive
      {message, Dest, From, Pid, Trace} ->
         handleMessage(RouterName, RoutingTable, Dest, From, Pid, Trace),
         loop(RouterName, RoutingTable);
      {control, From, Pid, SeqNum, ControlFun} ->
         handleControl(RouterName, RoutingTable, From, Pid, SeqNum, ControlFun),
         loop(RouterName, RoutingTable);
      {dump, From} ->
         handleDump(RoutingTable, From),
         loop(RouterName, RoutingTable);
      stop ->
         handleStop(RouterName, RoutingTable)
   end.


handleMessage (RouterName, RoutingTable, Dest, _, Pid, Trace) ->
   io:format("Received a message~n"),
   NewTrace = [RouterName|Trace],
   if
      Dest == RouterName ->
         Pid ! {trace, self(), lists:reverse(NewTrace)};
      true ->
         %% Check if Next is empty/wrong
         Next = ets:lookup(RoutingTable, Dest),
         if
            Next /= [] ->
               Next ! {message, Dest, self(), Pid, NewTrace};
            true ->
               %% Bad things have happened...
               io:format("Router ~p has received a message to ~p it can't route~n", [RouterName, Dest])
         end
   end.


handleControl (RouterName, RoutingTable, From, Pid, SeqNum, ControlFun) ->
   if SeqNum == 0 -> %% init
      Children = ControlFun(RouterName, RoutingTable),
      io:format("Children: ~p~n", [Children]),
      if Children == abort ->
            body;
         true ->
            body
      end;
   true ->
      %% not init
      body
   end.


handleDump (RoutingTable, From) ->
   io:format ("Taking a dump~n"),
   Dump = ets:match(RoutingTable, '$1'),
   From ! {table, self(), Dump}.


handleStop (RouterName, RoutingTable) ->
   ets:delete(RoutingTable),
   io:format ("Stopping router: ~p~n", [RouterName]).
