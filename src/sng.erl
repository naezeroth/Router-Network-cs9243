-module(sng).
-export([sng/0]).

sng () ->
  [{red, [{white, [white, green]},
	        {blue, [blue]}]},
   {white, [{red, [blue]},
	          {blue, [green, red]}]},
   {blue, [{green, [white, green, red]}]},
   {green, [{red, [red, blue, white]}]}
  ].
