-module(sng).
-export([sng4/0, sng6/0, sngr6/0]).

sng4 () ->
  [{red, [{white, [white, green]},
	        {blue, [blue]}]},
   {white, [{red, [blue]},
	          {blue, [green, red]}]},
   {blue, [{green, [white, green, red]}]},
   {green, [{red, [red, blue, white]}]}
  ].

sng6 () ->
  [{red   , [{white  , [white, blue, green, orange, yellow]}]},
   {white , [{blue   , [red, blue, green, orange, yellow]}]},
   {blue  , [{green  , [red, white, green, orange, yellow]}]},
   {green , [{orange , [red, white, blue, orange, yellow]}]},
   {orange, [{yellow , [red, white, blue, green, yellow]}]},
   {yellow, [{red    , [red, white, blue, green, orange]}]}
  ].

sngr6 () ->
  [{red   , [{yellow  , [white, blue, green, orange, yellow]}]},
   {white , [{red   , [red, blue, green, orange, yellow]}]},
   {blue  , [{white  , [red, white, green, orange, yellow]}]},
   {green , [{blue , [red, white, blue, orange, yellow]}]},
   {orange, [{green , [red, white, blue, green, yellow]}]},
   {yellow, [{orange    , [red, white, blue, green, orange]}]}
  ].