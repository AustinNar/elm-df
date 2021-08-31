module DataFrame exposing (DataFrame, DFResult, fromList, getSelected, getRows, select, map, add, mult, col, numLit, sortBy)

import Row exposing (Row)
import Field exposing (Calc, Field(..))
import List
import String
import Dict exposing (Dict)
import Set
import Maybe
import Result exposing (Result)





-- Custom Types

type DataFrame 
  = DataFrame ( List Row ) ( List String )


type alias DFResult = Result String DataFrame



fromList : List String -> List ( List Field ) -> DFResult
fromList names rows =
  let 
    head = List.head rows
  in
  case head of
    Nothing -> Ok ( DataFrame [] names )
    Just first ->
      let
        getType = \field ->
          case field of
            NumericField _ -> 0
            StringField _ -> 1
            UndefinedField _ -> 2
        getTypes = List.map getType
        firstTypes = getTypes first
        firstLength = List.length first
        namesLength = List.length names
      in
      if not ( List.all ( \row -> ( getTypes row ) == firstTypes ) rows ) then
        Err "Rows do not have consistent types."
      else if not ( List.all ( \row -> List.length row == firstLength ) rows ) then
        Err "Rows do not have a consistent number of fields."
      else if namesLength /= firstLength then
        Err ( "Rows have " ++ ( String.fromInt firstLength ) ++ " fields but " ++ ( String.fromInt namesLength ) ++ " field names were passed." )
      else
        Ok ( DataFrame ( List.map ( \row -> Row.fromList ( List.map2 ( \name field -> ( name, field ) ) names row ) ) rows ) names )

fields : Row -> List String
fields row = Dict.keys ( Row.toDict row )


-- Custom Functions for manipulating DataFrames

getRows : DataFrame -> List Row
getRows ( DataFrame rows _ ) = rows


getSelected : DataFrame -> List String
getSelected ( DataFrame _ selected ) = selected


errIfUndefined : DataFrame -> DFResult
errIfUndefined df =
  let
    undefined = 
      getRows df
      |> List.map ( Row.filter Field.isUndefined )
      |> List.map Row.toDict
      |> List.filter (not << Dict.isEmpty)
      |> List.head
      |> Maybe.withDefault Dict.empty
      |> Dict.values
      |> List.head
  in
    case undefined of
      Just field -> Err ( Field.toString field )
      Nothing -> Ok df



select : List String -> DataFrame -> DFResult
select cols df =
  let
    missing = List.head
      ( Set.toList 
        ( Set.diff 
          ( Set.fromList cols ) 
          ( Set.fromList 
            ( fields
              ( Maybe.withDefault 
                ( Row.fromDict Dict.empty )
                ( List.head ( getRows df ) ) 
              ) 
            )
          )  
        )
      )
  in
  case missing of 
    Nothing
      -> Ok ( DataFrame ( List.map (Row.select cols) ( getRows df ) ) cols )
    Just name
      -> Err ( "Unable to select " ++ name ++ " since it does not exist in the passed dataframe." )


map : String -> Calc Row -> DataFrame -> DFResult
map name calc df =
  let
    rows = getRows df
    selected = getSelected df
  in
  DataFrame 
  ( List.map ( Row.map name calc ) ( getRows df ) )
  ( if List.member name selected then
      selected
    else
      selected ++ [ name ]
  )
  |> errIfUndefined


sortBy : Calc Row -> DataFrame -> DFResult
sortBy calc df =
  let
    rows = getRows df
    cols = getSelected df
  in
    Ok ( DataFrame ( List.sortWith ( Row.comp calc ) rows ) cols )


add : Calc a -> Calc a -> Calc a
add left right data =
  let
    leftField = left data
    rightField = right data
  in
  case ( leftField, rightField ) of
    ( NumericField leftNum, NumericField rightNum )
      -> NumericField ( leftNum + rightNum )
    ( UndefinedField reason, _ ) 
      -> UndefinedField reason
    ( _, UndefinedField reason )
      -> UndefinedField reason
    _
      -> UndefinedField "ADD only accepts two NumericFields"


mult : Calc a -> Calc a -> Calc a
mult left right data =
  let
    leftField = left data
    rightField = right data
  in
  case ( leftField, rightField ) of
    ( NumericField leftNum, NumericField rightNum )
      -> NumericField ( leftNum * rightNum )
    ( UndefinedField reason, _ ) 
      -> UndefinedField reason
    ( _, UndefinedField reason )
      -> UndefinedField reason
    _
      -> UndefinedField "ADD only accepts two NumericFields"


col : String -> Calc Row
col name row =
  Row.get name row


numLit : Float -> Calc a
numLit num data =
  NumericField num


strLit : String -> Calc a
strLit str data =
  StringField str