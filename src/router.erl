-module(router).
-export([start/1]).

start (RouterName) ->
    io:format ("Starting router: ~p~n", [RouterName]),
    spawn(fun() -> init(RouterName) end).

init(RouterName) ->
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

handleMessage (RouterName, RoutingTable, Dest, From, Pid, Trace) ->
    io:format("Received a message"),
    NewTrace = [RouterName|Trace],
    if
        Dest == RouterName ->
            Pid ! {trace, self(), lists:reverse(NewTrace)};
        true ->
            %% Check if Next is empty/wrong
            Next = ets:lookup(RoutingTable, Dest),
            Next ! {message, Dest, self(), Pid, NewTrace}
    end.

handleControl (RouterName, RoutingTable, From, Pid, SeqNum, ControlFun) ->
        if
            SeqNum == 0 ->
                %% init
                Children = ControlFun(RouterName, RoutingTable),
                body;
            true ->
                %% not init
                body
        end.

handleDump (RoutingTable, From) ->
    io:format ("Taking a dump"),
    Dump = ets:match(RoutingTable, '$1'),
    From ! {table, self(), Dump}.

handleStop (RouterName, RoutingTable) ->
    io:format ("Stopping router: ~p~n", [RouterName]).
