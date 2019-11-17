-module(modifyTest).
-export([runTest/0]).

% c(control). c(router). c(sng). c(networkTest). c(modifyTest). modifyTest:runTest().

%% Circular network consisting of three nodes.
%%
circularNetwork6 () ->
  [{red   , [{white  , [white, blue, green, orange, yellow]}]},
   {white , [{blue   , [red, blue, green, orange, yellow]}]},
   {blue  , [{green  , [red, white, green, orange, yellow]}]},
   {green , [{orange , [red, white, blue, orange, yellow]}]},
   {orange, [{yellow , [red, white, blue, green, yellow]}]},
   {yellow, [{red    , [red, white, blue, green, orange]}]}
  ].

%% Reverse circle as in circularNetwork6/0
%%
reverseCircularNetwork6 () ->
  [{red   , [{yellow  , [white, blue, green, orange, yellow]}]},
   {white , [{red   , [red, blue, green, orange, yellow]}]},
   {blue  , [{white  , [red, white, green, orange, yellow]}]},
   {green , [{blue , [red, white, blue, orange, yellow]}]},
   {orange, [{green , [red, white, blue, green, yellow]}]},
   {yellow, [{orange    , [red, white, blue, green, orange]}]}
  ].

%% Modified circle as in circularNetwork6/0
%%
modifiedCircularNetwork6 () ->
  [{red   , [{blue   , [white, blue, green, orange, yellow]}]},
   {white , [{red    , [red, blue, green, orange, yellow]}]},
   {blue  , [{white  , [red, white]}, {green, [green, orange, yellow]}]},
   {green , [{yellow , [red, white, blue, orange, yellow]}]},
   {orange, [{green , [red, white, blue, green, yellow]}]},
   {yellow, [{red , [red, white, blue]}, {orange, [green, orange]}]}
  ].

%% Build a circular network and then use a control request to change the
%% direction of all edges; verify the original and reversed network
%%
runTest () ->
  io:format ("Starting network~n"),
  CGraph = circularNetwork6 (),
  RedPid = control:graphToNetwork (CGraph),
  networkTest:verifyNetwork (RedPid, CGraph),

  {WhitePid, _} = networkTest:probeNetwork (RedPid, white),
  {BluePid , _} = networkTest:probeNetwork (RedPid, blue ),
  {GreenPid , _} = networkTest:probeNetwork (RedPid, green ),
  {OrangePid , _} = networkTest:probeNetwork (RedPid, orange ),
  {YellowPid , _} = networkTest:probeNetwork (RedPid, yellow ),
  if (WhitePid == undef) or (BluePid == undef) or (GreenPid == undef) or (OrangePid == undef) or (YellowPid == undef) ->
      io:format ("*** ERROR: Corrupt network!~n");
     true -> true
  end,

  io:format ("*** Modifying cycle...~n"),
  RedPid ! {control, self (), self (), 1,
	    fun (Name, Table) ->
		case Name of
		  red    -> ets:insert (Table, [{white, BluePid },{blue, BluePid },{green, BluePid },{orange, BluePid },{yellow, BluePid}]);
		  white  -> ets:insert (Table, [{red, RedPid    },{blue, RedPid    },{green, RedPid    },{orange, RedPid    },{yellow, RedPid}]);
		  blue   -> ets:insert (Table, [{white, WhitePid  },{red, WhitePid  },{green, GreenPid  },{orange, GreenPid  },{yellow, GreenPid}]);
		  green  -> ets:insert (Table, [{white, YellowPid   },{blue, YellowPid   },{red, YellowPid   },{orange, YellowPid   },{yellow, YellowPid}]);
		  orange -> ets:insert (Table, [{white, GreenPid  },{blue, GreenPid  },{green, GreenPid  },{red, GreenPid  },{yellow, GreenPid}]);
		  yellow -> ets:insert (Table, [{white, RedPid },{blue, RedPid },{green, OrangePid },{orange, OrangePid },{red, RedPid}])
		end,
		[]
	    end},

  receive
      {committed, RedPid, 1} -> io:format ("*** ...done.~n");
      {abort    , RedPid, 1} -> io:format ("*** ERROR: Re-configuration failed!~n")
  after 10000 -> io:format ("*** ERROR: Re-configuration timed out!~n")
  end,
  
  RedPid ! {dump, self ()},
  receive 
    {table, RedPid, Table1} ->
      io:format ("*** Table Dump of red:~n"),
      io:format ("~w~n", [Table1])
  after 10000 -> io:format ("!!! Can't obtain dump~n")
  end,
  BluePid ! {dump, self ()},
  receive 
    {table, BluePid, Table2} ->
      io:format ("*** Table Dump of blue:~n"),
      io:format ("~w~n", [Table2])
  after 5000 -> io:format ("!!! Can't obtain dump~n")
  end,
  WhitePid ! {dump, self ()},
  receive 
    {table, WhitePid, Table3} ->
      io:format ("*** Table Dump of white:~n"),
      io:format ("~w~n", [Table3])
  after 5000 -> io:format ("!!! Can't obtain dump~n")
  end,
  %% networkTest:verifyNetwork (RedPid, modifiedCircularNetwork6 ()).
%   networkTest:verifyNetwork (RedPid, CGraph),
  networkTest:verifyNetwork (RedPid, modifiedCircularNetwork6 ()).
