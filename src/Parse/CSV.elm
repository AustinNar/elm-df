module Parse.CSV exposing (fromCSV)

import DataFrame as DF exposing (DataFrame)
import Field exposing (..)
import Parser exposing ((|.), (|=), Parser, chompWhile, float, getChompedString, keyword, map, oneOf, sequence, spaces, succeed, symbol, Trailing(..))
import Row

negatableFloat : Parser Float
negatableFloat =
  oneOf
    [ succeed negate
        |. symbol "-"
        |= float
    , float
    ]

field : Parser Field
field =
    let
        stop =
            \c -> List.member c [ ',', '\n' ]
    in
    oneOf
        [ map NumericField negatableFloat
        , map (\_ -> BoolField True) (oneOf [ keyword "true", keyword "True" ])
        , map (\_ -> BoolField False) (oneOf [ keyword "False", keyword "False" ])
        , map (\_ -> NothingField) (keyword "")
        , map StringField (getChompedString <| chompWhile <| not << stop)
        ]


fieldName : Parser String
fieldName =
    let
        stop =
            \c -> List.member c [ ',', '\n' ]
    in
        getChompedString <| chompWhile <| not << stop


header : Parser (List String)
header = sequence
  { start = ""
  , separator = ","
  , end = ""
  , spaces = succeed ()
  , item = fieldName
  , trailing = Forbidden
  }


row : Parser (List Field)
row = sequence
  { start = ""
  , separator = ","
  , end = ""
  , spaces = succeed ()
  , item = field
  , trailing = Forbidden
  }

body : Parser (List (List Field))
body = sequence
  { start = ""
  , separator = "\n"
  , end = ""
  , spaces = succeed ()
  , item = row
  , trailing = Optional
  }


dataframe : Parser DataFrame
dataframe = 
    succeed DF.fromList
        |= header
        |. symbol "\n"
        |= body

fromCSV : String -> DataFrame
fromCSV contents =
    let
        parsed = Parser.run dataframe contents
    in
        case parsed of
            Ok df ->
                df
            Err deadEndList ->
                DF.fromError <| Parser.deadEndsToString deadEndList
