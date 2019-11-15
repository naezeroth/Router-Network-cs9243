-module(control).
-export([graphToNetwork/1, extendNetwork/4]).


graphToNetwork(Graph) ->
   Routers = ets:new(undef, [private]),
   InboundCount = ets:new(undef, [private]),
   % create and count procs
   spawnRouters(Graph, InboundCount, Routers),
   % At this point every router should exist and we can send init messages
   initRouters(Graph, Routers, InboundCount),
   io:format("Inbound ~p~n", [ets:match(InboundCount, '$1')]),
   io:format("Routers ~p~n", [ets:match(Routers, '$1')]),
   ets:delete(InboundCount),
   ets:delete(Routers).


initRouters(Graph, Routers, InboundCount) ->
   lists:foreach(
      fun({RouterName, Edges}) ->
         io:format("Router ~p has edges ~p~n", [RouterName, Edges]),
         Routes = mapRoutes(Edges, Routers, []),
         [{_, Inbound}] = ets:lookup(InboundCount, RouterName),
         [{_, Pid}] = ets:lookup(Routers, RouterName),
         Pid ! {control, self(), self(), 0,
            fun(_, Table) ->
               ets:insert(Table, Routes),
               ets:insert(Table, {'$NoInEdges', Inbound})
            end}
      end, Graph).


spawnRouters(Graph, InboundCount, Routers) ->
   lists:foreach(
      fun({RouterName, Edges}) -> 
         Pid = router:start(RouterName),
         ets:insert(Routers, {RouterName, Pid}),
         % Counting direct connections of routers will give the incoming edges
         countDirect(Edges, InboundCount)
      end, Graph).


countDirect(Edges, InboundCount) ->
   lists:foreach(
      fun(Edge) ->
         {Hop, _} = Edge,
         Inbound = ets:lookup(InboundCount, Hop),
         if Inbound == [] -> ets:insert(InboundCount, {Hop, 1});
            true ->
               [{_, N}] = Inbound, % list of tuples returned
               ets:insert(InboundCount, {Hop, N + 1})
         end
      end, Edges).


mapRoutes([], _, RouteMap) ->
   RouteMap;
mapRoutes([{Hop, Dests}|T], Routers, RouteMap) ->
   % Should this be guarded?
   [{_,Pid}] = ets:lookup(Routers, Hop),
   Routes = lists:map(fun(Dest) -> {Dest, Pid} end, Dests),
   mapRoutes(T, Routers, RouteMap ++ Routes).


extendNetwork(RootPid, SeqNum, From, EdgeMap) ->
   wip.
