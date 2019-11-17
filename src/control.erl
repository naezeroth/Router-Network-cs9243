-module(control).
-export([graphToNetwork/1, extendNetwork/4]).

debug() -> nope. % 'debug' for debug mode, else anything

graphToNetwork(Graph) ->
   Routers = ets:new(undef, [private]),
   InboundCount = ets:new(undef, [private]),
   % create and count procs
   Spawned = spawnRouters(Graph, InboundCount, Routers),
   % At this point every router should exist and we can send init messages
   initRouters(Graph, Routers, InboundCount),
   GoodToGo = confirmInitialized(Spawned, 0, allGood),
   case debug() of debug -> io:format("Inbound ~p~n", [ets:match(InboundCount, '$1')]); _ -> ignore end,
   case debug() of debug -> io:format("Routers ~p~n", [ets:match(Routers, '$1')]); _ -> ignore end,
   [FirstRouter|_] = Graph,
   {RootNode,_} = FirstRouter,
   [{_, RootPid}] = ets:lookup(Routers, RootNode),
   ets:delete(InboundCount),
   ets:delete(Routers),
   case debug() of debug -> io:format("~n~n~n"); _ -> ignore end,
   io:format("~p~n", [GoodToGo]),
   case GoodToGo of
      allGood -> RootPid;
      _ -> failed
   end.


spawnRouters(Graph, InboundCount, Routers) ->
   lists:foreach(
      fun({RouterName, Edges}) -> 
         Pid = router:start(RouterName),
         ets:insert(Routers, {RouterName, Pid}),
         % Counting direct connections of routers will give the incoming edges
         countDirect(Edges, InboundCount)
      end, Graph),
      length(Graph).


initRouters(Graph, Routers, InboundCount) ->
   lists:foreach(
      fun({RouterName, Edges}) ->
         case debug() of debug -> io:format("Router ~p has edges ~p~n", [RouterName, Edges]); _ -> ignore end,
         Routes = mapRoutes(Edges, Routers, [], byName),
         case debug() of debug -> io:format("Routes: ~p ~n", [Routes]); _ -> ignore end,
         [{_, Inbound}] = ets:lookup(InboundCount, RouterName),
         [{_, Pid}] = ets:lookup(Routers, RouterName),
         Pid ! {control, self(), self(), 0,
            fun(_, Table) ->
               ets:insert(Table, Routes),
               ets:insert(Table, {'$NoInEdges', Inbound}),
               []
            end}
      end, Graph).


confirmInitialized(ExpectedResponses, CurrentResponses, MyReturn)->
   receive
      {committed, _, 0} ->
         case CurrentResponses + 1 of
            ExpectedResponses -> MyReturn;
            _ -> confirmInitialized(ExpectedResponses, CurrentResponses + 1, MyReturn)
         end;
      {abort, _, 0} ->
            case CurrentResponses + 1 of
               ExpectedResponses -> ohNoNoNO;
               _ -> confirmInitialized(ExpectedResponses, CurrentResponses + 1, ohNoNoNO)
            end
   after 1000 -> io:format("Control timeout ~p => ~p~n", [CurrentResponses, ExpectedResponses])
   end.


countDirect(Edges, InboundCount) ->
   lists:foreach(
      fun({Hop, _}) ->
         Inbound = ets:lookup(InboundCount, Hop),
         case Inbound == [] of 
            true ->
               ets:insert(InboundCount, {Hop, 1});
            false ->
               [{_, N}] = Inbound, % list of tuples returned
               ets:insert(InboundCount, {Hop, N + 1})
         end
      end, Edges).


% Pattern matching is fun...Maybe too much fun
mapRoutes([], _, RouteMap, _) ->
   RouteMap;
mapRoutes([{Hop, Dests}|T], Routers, RouteMap, byName) ->
   [{_, Pid}] = ets:lookup(Routers, Hop),
   Routes = [{Dest, Pid} || Dest <- Dests],
   mapRoutes(T, Routers, RouteMap ++ Routes, byName);
mapRoutes([{Pid, Dests}|T], _, RouteMap, byPid) ->
   Routes = [{Dest, Pid} || Dest <- Dests],
   mapRoutes(T, nothing, RouteMap ++ Routes, byPid).

extendNetwork(RootPid, SeqNum, From, {NodeName, EdgeMap}) ->
   Routes = mapRoutes(EdgeMap, nothing, [], byPid),
   ControlPid = self(),
   ControlFun =
      fun(Name, Table) ->
         case From of
            Name -> % Spwan new process here
               SpawnFun = fun() ->
                  Pid = router:start(NodeName),
                  Pid ! {control, self(), ControlPid, 0,
                     fun(_, Table2) ->
                        ets:insert(Table2, Routes),
                        ets:insert(Table2, {'$NoInEdges', 1}) % Only From is an inbound link
                     end},
                  ets:insert(Table, {NodeName, Pid}),
                  Pid % Return Pid from SpawnFun to ControlFun
               end,
               try SpawnFun() of
                  Pid -> [Pid] % Return list of spawned routers to From router
               catch
                  _:_ -> abort % Catch all
               end;
            _ -> % No process spawned, but still need to update routing table
               % 1. Find out how to get to From, then use that same hop to get to NodeName
               [{_, Hop}] = ets:lookup(Table, From),
               ets:insert(Table, {NodeName, Hop}),
               % 2. if NodeName has a link to this router, update our inocming links
            lists:foreach(
               fun({Pid, _}) ->
                  MyPid = self(),
                  case Pid of
                     MyPid ->
                        [{_, Inbound}] = ets:lookup(Table, '$NoInEdges'),
                        ets:insert(Table, {'$NoInEdges', Inbound + 1});
                     _ -> ignore
                  end
               end, EdgeMap),
            [] % Return no new routers spawned
         end
      end,
   RootPid ! {control, self(), self(), SeqNum, ControlFun},
   receive
      % Trap the committed response of the new node
      {committed, _, 0} ->
         io:format("Received commit in extend~n"),
         ok;
      {abort, _, 0} ->
         ok
   after 100 -> ok
   end,

   receive
      % Our only communication is true/false, so really we're only looking for committed/abort
      {committed, _, SeqNum2} ->
            io:format("Received commit in extend for seq ~p~n", [SeqNum2]),
         true;
      {abort, _, _} ->
         false
   end.
