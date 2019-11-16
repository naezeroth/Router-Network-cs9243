-module(concurrentTest).
-export([runTest/0]).

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

%% Build a circular network and then use a control request to change the
%% direction of all edges; verify the original and reversed network
%%
runTest () ->
  io:format ("*** Starting router network...~n"),
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

  io:format ("*** Reversing routing cycle...~n"),
  RedPid ! {control, self (), self (), 1,
	    fun (Name, Table) ->
		case Name of
		  red    -> ets:insert (Table, [{white, YellowPid },{blue, YellowPid },{green, YellowPid },{orange, YellowPid },{yellow, YellowPid }]);
		  white  -> ets:insert (Table, [{white, RedPid    },{blue, RedPid    },{green, RedPid    },{orange, RedPid    },{yellow, RedPid    }]);
		  blue   -> ets:insert (Table, [{white, WhitePid  },{blue, WhitePid  },{green, WhitePid  },{orange, WhitePid  },{yellow, WhitePid  }]);
		  green  -> ets:insert (Table, [{white, BluePid   },{blue, BluePid   },{green, BluePid   },{orange, BluePid   },{yellow, BluePid   }]);
		  orange -> ets:insert (Table, [{white, GreenPid  },{blue, GreenPid  },{green, GreenPid  },{orange, GreenPid  },{yellow, GreenPid  }]);
		  yellow -> ets:insert (Table, [{white, OrangePid },{blue, OrangePid },{green, OrangePid },{orange, OrangePid },{yellow, OrangePid }])
		end,
		[]
	    end},

  receive
      {committed, RedPid, 1} -> io:format ("*** ...done.~n");
      {abort    , RedPid, 1} ->
          io:format ("*** ERROR: Re-configuration failed!~n")
  after 10000              ->
            io:format ("*** ERROR: Re-configuration timed out!~n")
  end,

  receive
      {committed, BluePid, 2} -> io:format ("*** ...done.~n");
      {abort    , BluePid, 2} ->
          io:format ("*** ERROR: Re-configuration failed!~n")
  after 10000              ->
            io:format ("*** ERROR: Re-configuration timed out!~n")
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
  %% networkTest:verifyNetwork (RedPid, reverseCircularNetwork6 ()).
  networkTest:verifyNetwork (RedPid, CGraph),

  io:format ("*** Reversing routing cycle...~n"),
  RedPid ! {control, self (), self (), 5, 
      fun (Name, Table) -> 
    case Name of
      red   -> ets:insert (Table, [{white, BluePid }, 
                 {blue , BluePid }]);
      white -> ets:insert (Table, [{red  , RedPid  }, 
                 {blue , RedPid  }]);
      blue  -> ets:insert (Table, [{red  , WhitePid}, 
                 {white, WhitePid}])
    end,
    []
      end},
  receive
    {committed, RedPid, 5} -> io:format ("*** ...done.~n");
    {abort    , RedPid, 5} -> 
      io:format ("*** ERROR: Re-configuration failed!~n")
  after 10000              ->
      io:format ("*** ERROR: Re-configuration timed out!~n")
  end,
  networkTest:verifyNetwork (RedPid, reverseCircularNetwork6 ()).
