-module(simultaniousControlTest).
-export([runTest/0]).

%% Circular network consisting of three nodes.
%%
circularNetwork3 () ->
  [{red  , [{white, [white, blue]}]},
   {white, [{blue , [red, blue]}]},
   {blue , [{red  , [red, white]}]}
  ].

%% Reverse circle as in circularNetwork3/0
%%
extendedCircularNetwork3 () ->
  [{red  , [{blue , [white, blue]}]},
   {white, [{red  , [red, blue]}]},
   {blue , [{white, [red, white]}]}
  ].

%% Build a circular network and then use a control request to change the
%% direction of all edges; verify the original and reversed network
%%
runTest () ->
  io:format ("*** Starting router network...~n"),
  CGraph = circularNetwork3 (),
  RedPid = control:graphToNetwork (CGraph),
  networkTest:verifyNetwork (RedPid, CGraph),

  {WhitePid, _} = networkTest:probeNetwork (RedPid, white),
  {BluePid , _} = networkTest:probeNetwork (RedPid, blue ),
  if (WhitePid == undef) or (BluePid == undef) ->
      io:format ("*** ERROR: Corrupt network!~n");
     true -> true
  end,

  io:format("red ~w, white ~w, blue ~w~n", [RedPid, WhitePid, BluePid]),

  io:format ("*** Extending two at same time...~n"),
  
  % WhitePid ! {control, self (), self (), 1,
  %      fun (Name, Table) ->
  %     case Name of
  %       red   -> ets:insert (Table, [{white, WhitePid },
  %                     {blue , WhitePid }]);
  %       white -> timer:sleep(3000),
  %              ets:insert (Table, [{red  , BluePid  },
  %                     {blue , BluePid  }]);
  %       blue  -> ets:insert (Table, [{red  , RedPid},
  %                     {white, RedPid}])
  %     end,
  %     []
  %      end},
  % RedPid ! {control, self (), self (), 2,
  %      fun (Name, Table) ->
  %     case Name of
  %       red   -> timer:sleep(2000),
  %             ets:insert (Table, [{white, BluePid },
  %                     {blue , BluePid }]);
  %       white -> ets:insert (Table, [{red  , RedPid  },
  %                     {blue , RedPid  }]);
  %       blue  -> ets:insert (Table, [{red  , WhitePid},
  %                     {white, WhitePid}])
  %     end,
  %     []
  %      end},
  CommitCount1 = receive
    {committed, RedPid, 2} -> 1;
    {abort    , RedPid, 2} -> 0
  after 10000              ->
      io:format ("*** ERROR: Re-configuration timed out!~n")
  end,
  CommitCount2 = CommitCount1 + receive
    {committed, WhitePid, 1} -> 2;
    {abort    , WhitePid, 1} -> 0
  after 10000              ->
      io:format ("*** ERROR: Re-configuration timed out!~n")
  end,
  if
  CommitCount2 == 3 ->
    io:format("*** ERROR: Meh! commited both!~n");
  true ->
    io:format("*** committed: ~w~n", [CommitCount2])
  end,
  if
  % CommitCount2 == 1 ->
    % networkTest:verifyNetwork (RedPid, reverseCircularNetwork3 ());
  CommitCount2 /= 1 ->
    networkTest:verifyNetwork (RedPid, circularNetwork3 ())
  end.
