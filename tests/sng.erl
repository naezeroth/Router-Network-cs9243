-module(sng).
-export([sml4/0, med6/0, med6r/0, lrg11/0, trie7/0]).

sml4 () ->
  [{red, [{white, [white, green]},
	        {blue, [blue]}]},
   {white, [{red, [blue]},
	          {blue, [green, red]}]},
   {blue, [{green, [white, green, red]}]},
   {green, [{red, [red, blue, white]}]}
  ].

med6 () ->
  [{red   , [{white  , [white, blue, green, orange, yellow]}]},
   {white , [{blue   , [red, blue, green, orange, yellow]}]},
   {blue  , [{green  , [red, white, green, orange, yellow]}]},
   {green , [{orange , [red, white, blue, orange, yellow]}]},
   {orange, [{yellow , [red, white, blue, green, yellow]}]},
   {yellow, [{red    , [red, white, blue, green, orange]}]}
  ].

med6r () ->
  [{red   , [{yellow  , [white, blue, green, orange, yellow]}]},
   {white , [{red   , [red, blue, green, orange, yellow]}]},
   {blue  , [{white  , [red, white, green, orange, yellow]}]},
   {green , [{blue , [red, white, blue, orange, yellow]}]},
   {orange, [{green , [red, white, blue, green, yellow]}]},
   {yellow, [{orange    , [red, white, blue, green, orange]}]}
  ].

lrg11 () ->
  [{red    , [{blue, [blue, green, orange, magenta, yellow, cyan, black, white, grey, sepia]}]},
   {blue   , [{green, [green, white]}, {orange, [red, orange, magenta, yellow, cyan, black, grey, sepia]}]},
   {green  , [{white, [red, blue, orange, magenta, yellow, cyan, black, white, grey, sepia]}]},
   {orange , [{magenta, [cyan, magenta, black, grey]}, {yellow, [red, blue, green, yellow, white, sepia]}]},
   {magenta, [{black, [red, blue, green, yellow, cyan, black, sepia]}, {grey, [white, grey, orange]}]},
   {yellow , [{sepia, [red, blue, green, orange, magenta, cyan, black, white, grey, sepia]}]},
   {cyan   , [{yellow, [red, blue, green, orange, magenta, yellow, black, white, grey, sepia]}]},
   {black  , [{cyan, [red, blue, green, orange, magenta, yellow, cyan, white, grey, sepia]}]},
   {white  , [{orange, [red, blue, green, orange, magenta, yellow, cyan, black, grey, sepia]}]},
   {grey   , [{white, [red, blue, green, orange, magenta, yellow, cyan, black, white, sepia]}]},
   {sepia  , [{red, [red, blue, green, orange, magenta, yellow, cyan, black, white, grey]}]}
  ].

trie7 () ->
    [{red   , [{blue, [blue, white, green]}, {yellow, [yellow, magenta, orange]}]},
    {blue   , [{white, [white]}, {green, [red, yellow, green, magenta, orange]}]},
    {yellow , [{magenta, [magenta]}, {green, [red, blue, white, green, orange]}]},
    {white  , [{orange, [red, blue, yellow, green, magenta, orange]}]},
    {green  , [{orange, [red, blue, yellow, white, magenta, orange]}]},
    {magenta, [{orange, [red, blue, yellow, white, green, orange]}]},
    {orange , [{red, [red, blue, yellow, white, green, magenta]}]}
   ].