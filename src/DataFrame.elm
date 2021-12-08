module DataFrame exposing (DataFrame, SortOrder(..), fromError, add, col, divide, empty, fromList, getRows, getSelected, getError, andThen, join, gt, map, multiply, numLit, select, sortBy, strLit, subtract)

import Dict exposing (Dict)
import Field exposing (Calc, Field(..))
import List
import Maybe
import Result exposing (Result)
import Row exposing (Row)
import Set
import String


-- Custom Types

type alias DFRecord = 
    { rows: List Row
    , selected: List String
    , error: Maybe String 
    }

type DataFrame
    = DataFrame 
        { rows: List Row
        , selected: List String
        , error: Maybe String 
        }
    | GroupedDataFrame 
        { groups: List ( List Row ) 
        , selected: List String
        , groupingCols: List String
        , error: Maybe String 
        }


type SortOrder
    = ASC
    | DESC


empty : DataFrame
empty = DataFrame { rows = [], selected = [], error = Nothing }


fromError : String -> DataFrame
fromError msg = DataFrame { rows = [], selected = [], error = Just msg }


andThen : ( DataFrame -> DataFrame ) -> DataFrame -> DataFrame
andThen func df = 
    case ( getError df ) of
        Just error ->
            fromError error
        Nothing ->
            df


fromList : List String -> List (List Field) -> DataFrame
fromList names rows =
    let
        head =
            List.head rows
    in
    case head of
        Nothing ->
            DataFrame { rows = [], selected =  names, error = Nothing }

        Just first ->
            let
                getType =
                    \field ->
                        case field of
                            NumericField _ ->
                                0

                            StringField _ ->
                                1

                            BoolField _ ->
                                2

                            NothingField ->
                                3

                            UndefinedField _ ->
                                4

                getTypes =
                    List.map getType

                nothingType = 
                    getType NothingField

                accumulateType = 
                    \soFar next ->
                        Set.filter ((/=) nothingType) (Set.union next soFar)

                accumulateTypes = 
                    \soFar next ->
                        List.map2 accumulateType soFar next

                accumulatedTypes = 
                    List.foldl accumulateTypes (List.map (\_ -> Set.empty) first) (List.map (getTypes >> List.map Set.singleton) rows)

                firstLength =
                    List.length first

                namesLength =
                    List.length names

                
            in
            if not (List.all (Set.size >> (<=) 1) accumulatedTypes) then
                fromError "Rows do not have consistent types."

            else if not (List.all (List.length >> (==) firstLength) rows) then
                fromError "Rows do not have a consistent number of fields."

            else if namesLength /= firstLength then
                fromError ("Rows have " ++ String.fromInt firstLength ++ " fields but " ++ String.fromInt namesLength ++ " field names were passed.")

            else
                DataFrame 
                { rows = (List.map (Row.fromList << List.map2 (\name field -> ( name, field )) names) rows) 
                , selected = names 
                , error = Nothing
                }


fields : Row -> List String
fields row =
    Dict.keys (Row.toDict row)


-- Custom Functions for manipulating DataFrames


getRows : DataFrame -> List Row
getRows df =
    case df of
        DataFrame { rows } ->
            rows
        GroupedDataFrame { groups } ->
            List.foldl (++) [] groups 


getSelected : DataFrame -> List String
getSelected df =
    case df of
        DataFrame { selected } ->
            selected
        GroupedDataFrame { selected } ->
            selected


getError : DataFrame -> Maybe String
getError df =
    case df of
        DataFrame { error } ->
            error
        GroupedDataFrame { error } ->
            error


errIfUndefined : DataFrame -> DataFrame
errIfUndefined df =
    let
        undefined =
            getRows df
                |> List.map (Row.filter Field.isUndefined)
                |> List.map Row.toDict
                |> List.filter (not << Dict.isEmpty)
                |> List.head
                |> Maybe.withDefault Dict.empty
                |> Dict.values
                |> List.head
    in
    case undefined of
        Just field ->
            fromError (Field.toString field)

        Nothing ->
            df


select : List String -> DataFrame -> DataFrame
select cols df =
    let
        missing =
            getSelected df
                |> Set.fromList
                |> Set.diff (Set.fromList cols)
                |> Set.toList
                |> List.head
    in
    case missing of
        Nothing ->
            DataFrame 
            { rows = (List.map (Row.select cols) (getRows df)) 
            , selected = cols
            , error = Nothing
            }

        Just name ->
            fromError ("Unable to select " ++ name ++ " since it does not exist in the passed dataframe.") 

union : DataFrame -> DataFrame -> DataFrame
union df1 df2 = 
    let 
        selected1 = getSelected df1

        selected2 = getSelected df2

        matchingCols = ( Set.fromList selected1 ) == ( Set.fromList selected2 )
    in
    if matchingCols then
        case (df1, df2) of
            ( DataFrame { rows1 }, DataFrame { rows2 } ) ->
                DataFrame ( rows1 ++ rows2 ) selected1 Nothing
            _ ->
                fromError "Only ungrouped DataFrames can be unioned"
    else
        fromError "Only DataFrames with matching columns can be unioned"


map : String -> Calc Row -> DataFrame -> DataFrame
map name calc df =
    let
        rows =
            getRows df

        selected =
            getSelected df
    in
    DataFrame
        { rows = (List.map (Row.map name calc) (getRows df))
        , selected = 
            (if List.member name selected then
                selected

             else
                selected ++ [ name ]
            )
        , error = Nothing
        }
        |> errIfUndefined


agg : List ( String, Calc DataFrame ) -> DataFrame -> DataFrame
agg calcs df =
    let
        rowsList = 
            case df of
                DataFrame { rows } ->
                    [ rows ]
                GroupedDataFrame { groups } ->
                    groups

        grouping = 
            case df of
                DataFrame _ ->
                    []
                GroupedDataFrame { groupingCols } ->
                    groupingCols

        newCols = 
            List.map (\(name, _) -> name) calcs

        selected = 
            grouping ++ newCols

        mapTuple =
            \(name, calc) -> map name calc

        aggOne = 
            \rows ->
                List.foldl mapTuple ( DataFrame { rows = rows, selected = selected, error = Nothing } ) calcs

        aggedDfs = 
            List.map aggOne rowsList |> List.map ( select selected )

        seedDf =
            DataFrame { rows = [], selected = selected, error = Nothing }


    in
    List.foldl union seedDf aggedDfs |> errIfUndefined


sortBy : List (Calc Row) -> SortOrder -> DataFrame -> DataFrame
sortBy calcs sortorder df =
    let
        rows =
            getRows df

        cols =
            getSelected df

        reverse =
            \order ->
                case order of
                    GT ->
                        LT

                    EQ ->
                        EQ

                    LT ->
                        GT

        compOne =
            \calc -> 
                case sortorder of
                    ASC ->
                        Row.comp calc

                    DESC ->
                        \left right -> reverse (Row.comp calc left right)

        notEq = 
            \order ->
                case order of
                    EQ ->
                        False
                    _ -> 
                        True

        compAll = 
            \left right ->
                List.map (\calc -> compOne calc left right) calcs 
                    |> List.filter notEq
                    |> List.head
                    |> Maybe.withDefault EQ
    in
    DataFrame { rows = List.sortWith compAll rows, selected = cols, error =  Nothing }


join : String -> List (Calc Row) -> DataFrame -> DataFrame -> DataFrame
join how on left right =
    let
        (build,probe) = 
            if ( List.member how ["right", "rightsemi", "rightanti"] ) then
                (left,right)
            else
                (right,left)

        probeCols = 
            getSelected probe

        buildCols = 
            getSelected build
                |> List.filter ( \c -> not ( List.member c probeCols ) )

        nullBuild = 
            getSelected build 
                |> List.map (\c -> (c, NothingField)) 
                |> Row.fromList

        nullProbe = 
            getSelected probe
                |> List.map (\c -> (c, NothingField)) 
                |> Row.fromList

        mergeRows = 
            \probeRow buildRow ->
                Dict.union ( Row.toDict probeRow ) ( Row.toDict buildRow )
                    |> Row.fromDict

        hash = 
            \row -> List.map (\calc -> Field.hash (calc row)) on

        updateHash = 
            \(rowHash, row) dict ->
                Dict.get rowHash dict 
                    |> Maybe.withDefault []
                    |> (++) [row]
                    |> (\rows -> Dict.insert rowHash rows dict)

        buildHash = 
            List.foldl updateHash Dict.empty ( List.map (\row -> (hash row, row)) (getRows build) )

        createRows = 
            \(probeRow, buildRows) ->
                if ( List.member how ["left", "right"] ) then
                    buildRows
                        |> (\rows -> if List.isEmpty rows then [ nullBuild ] else rows)
                        |> List.map (\buildRow -> mergeRows probeRow buildRow )
                else if ( how == "inner" ) then
                    buildRows
                        |> List.map (\buildRow -> mergeRows probeRow buildRow )
                else if ( List.member how ["leftsemi", "rightsemi"] ) then
                    if List.isEmpty buildRows then
                        []
                    else
                        [ probeRow ]
                else if ( List.member how ["leftanti", "rightanti"] ) then
                    if List.isEmpty buildRows then
                        [ probeRow ]
                    else
                        []
                else 
                    [] 

        joinedRows = 
            getRows probe
                |> List.map (\probeRow -> ( probeRow, Dict.get (hash probeRow) buildHash |> Maybe.withDefault [] ))
                |> List.map createRows
                |> List.concat

        joinedCols = 
            if ( List.member how ["left", "right", "inner"] ) then
                probeCols ++ buildCols
            else if ( List.member how ["leftsemi", "rightsemi", "leftanti", "rightanti"] ) then
                probeCols
            else 
                []

    in
        if not ( List.member how [ "left", "right", "inner", "leftsemi", "rightsemi", "leftanti", "rightanti" ] ) then
            fromError "Join type must be one of left, right, inner, leftsemi, rightsemi, leftanti, rightanti."
        else 
            DataFrame 
            { rows = joinedRows 
            , selected = joinedCols 
            , error = Nothing
            }


add : Calc a -> Calc a -> Calc a
add left right data =
    let
        leftField =
            left data

        rightField =
            right data
    in
    case ( leftField, rightField ) of
        ( NumericField leftNum, NumericField rightNum ) ->
            NumericField (leftNum + rightNum)

        ( UndefinedField reason, _ ) ->
            UndefinedField reason

        ( _, UndefinedField reason ) ->
            UndefinedField reason

        _ ->
            UndefinedField "ADD only accepts two NumericFields"


subtract : Calc a -> Calc a -> Calc a
subtract left right data =
    let
        leftField =
            left data

        rightField =
            right data
    in
    case ( leftField, rightField ) of
        ( NumericField leftNum, NumericField rightNum ) ->
            NumericField (leftNum - rightNum)

        ( UndefinedField reason, _ ) ->
            UndefinedField reason

        ( _, UndefinedField reason ) ->
            UndefinedField reason

        _ ->
            UndefinedField "SUBTRACT only accepts two NumericFields"


multiply : Calc a -> Calc a -> Calc a
multiply left right data =
    let
        leftField =
            left data

        rightField =
            right data
    in
    case ( leftField, rightField ) of
        ( NumericField leftNum, NumericField rightNum ) ->
            NumericField (leftNum * rightNum)

        ( UndefinedField reason, _ ) ->
            UndefinedField reason

        ( _, UndefinedField reason ) ->
            UndefinedField reason

        _ ->
            UndefinedField "MULTIPLY only accepts two NumericFields"


divide : Calc a -> Calc a -> Calc a
divide left right data =
    let
        leftField =
            left data

        rightField =
            right data
    in
    case ( leftField, rightField ) of
        ( NumericField leftNum, NumericField rightNum ) ->
            NumericField (leftNum / rightNum)

        ( UndefinedField reason, _ ) ->
            UndefinedField reason

        ( _, UndefinedField reason ) ->
            UndefinedField reason

        _ ->
            UndefinedField "DIVIDE only accepts two NumericFields"


compMatch : List Order -> Calc a -> Calc a -> Calc a
compMatch matches left right data =
    let
        leftField =
            left data

        rightField =
            right data

        match =
            List.member (Field.comp leftField rightField) matches

        resultIfValid =
            BoolField match
    in
    case ( leftField, rightField ) of
        ( NumericField _, NumericField _ ) ->
            resultIfValid

        ( StringField _, StringField _ ) ->
            resultIfValid

        ( BoolField _, BoolField _ ) ->
            resultIfValid

        _ ->
            UndefinedField "Only pairs of NumericFields, StringFields, or BoolFields can be compared"


gt : Calc a -> Calc a -> Calc a
gt =
    compMatch [ GT ]


lt : Calc a -> Calc a -> Calc a
lt =
    compMatch [ LT ]


ge : Calc a -> Calc a -> Calc a
ge =
    compMatch [ GT, EQ ]


le : Calc a -> Calc a -> Calc a
le =
    compMatch [ LT, EQ ]


eq : Calc a -> Calc a -> Calc a
eq =
    compMatch [ EQ ]


ne : Calc a -> Calc a -> Calc a
ne =
    compMatch [ GT, LT ]


col : String -> Calc Row
col name row =
    Row.get name row


numLit : Float -> Calc a
numLit num data =
    NumericField num


strLit : String -> Calc a
strLit str data =
    StringField str
