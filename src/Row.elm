module Row exposing (..)


import Field exposing (..)
import Dict exposing (Dict)


type Row
  = Row (Dict String Field)


type alias Selected = List String



select : Selected -> Row -> Row
select cols ( Row row ) =
  Row ( Dict.filter ( \k v -> List.member k cols ) row )


filter : ( Field -> Bool ) -> Row -> Row
filter predicate ( Row row ) =
  Row ( Dict.filter ( \k v -> predicate v ) row )


toDict : Row -> Dict String Field
toDict ( Row row ) = row


update : String -> Field -> Row -> Row
update col field ( Row row ) =
  Row ( Dict.insert col field row )


get : String -> Row -> Field
get name ( Row row ) =
  Maybe.withDefault ( UndefinedField ( "Field " ++ name ++ " not in dataframe." ) ) ( Dict.get name row )


map : String -> Calc Row -> Row -> Row
map col calc row = 
  update col ( calc row ) row


comp : Calc Row -> Row -> Row -> Order
comp calc left right =
  Field.comp ( calc left ) ( calc right )

