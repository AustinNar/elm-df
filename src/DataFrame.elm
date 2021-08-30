module DataFrame exposing (DataFrame, DFResult, fromRows, getSelected, getRows, select, map, add, mult, col, numLit, sortBy)

import Row exposing (Row(..))
import Field exposing (Calc, Field(..))
import List
import String
import Dict exposing (Dict)
import Set
import Maybe
import Result exposing (Result)





-- Custom Types

type alias Selected = List String


type DataFrame 
  = DataFrame ( List Row ) Selected


fromRows : List Row -> DFResult
fromRows rows =
  let
    colList = 
      rows
      |> List.map Row.toDict 
      |> List.map Dict.keys
      |> List.map Set.fromList
    first = Maybe.withDefault Set.empty ( List.head colList )
  in
    if List.all ( \cols -> cols == first ) colList then
      Ok ( DataFrame rows (Set.toList first) )
    else
      Err "Rows do not have consistent set of fields"




type alias DFResult = Result String DataFrame



fields : Row -> Selected
fields ( Row rdict ) = Dict.keys rdict


-- Custom Functions for manipulating DataFrames

getRows : DataFrame -> List Row
getRows ( DataFrame rows _ ) = rows


getSelected : DataFrame -> Selected
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



select : Selected -> DataFrame -> DFResult
select cols df =
  let
    missing = List.head
      ( Set.toList 
        ( Set.diff 
          ( Set.fromList cols ) 
          ( Set.fromList 
            ( fields
              ( Maybe.withDefault 
                ( Row Dict.empty )
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