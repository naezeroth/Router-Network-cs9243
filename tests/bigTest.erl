-module(bigTest).
-export([runTest/0]).

%% Circular network consisting of three nodes.
%%
circularNetwork3 () ->
  [{a, [{c, [d, f, g, c]}, {b, [a, b, e]}]},
  {b, [{d, [a, b, c, d, e, f, g]}]},
  {c, [{d, [a, b, c, d, e, f, g]}]},
  {d, [{f, [g, c, f]}, {e, [e, b, d, a]}]},
  {e, [{g, [a, b, c, d, e, f, g]}]},
  {f, [{g, [a, b, c, d, e, f, g]}]},
  {g, [{a, [a, b, c, d, e, f, g]}]}
].


%% Build a circular network and then use a control request to change the
%% direction of all edges; verify the original and reversed network
%%
runTest () ->
  io:format ("*** Starting router network...~n"),
  CGraph = circularNetwork3 (),
  RootPid = control:graphToNetwork (CGraph),
  networkTest:verifyNetwork (RootPid, CGraph),

  io:format ("*** Reversing routing cycle...~n"),
  RootPid ! {control, self (), self (), 1,
	    fun (Name, _Table) ->
		case Name of
		  d   -> abort;
		  Name -> []
		end
	    end},
  receive
    {abort, RootPid, 1} -> io:format ("*** ...done.~n");
    {committed, RootPid, 1} ->
      io:format ("*** ERROR: Re-configuration failed!~n")
  after 10000              ->
      io:format ("*** ERROR: Re-configuration timed out!~n")
  end,
  networkTest:verifyNetwork (RootPid, circularNetwork3 ()).
