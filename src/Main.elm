module Main exposing (..)

import Browser
import DataFrame as DF exposing (DataFrame)
import Dict
import Field exposing (..)
import Html exposing (..)
import Parse.CSV as CSV
import Row exposing (..)



-- MAIN


main =
    Browser.sandbox { init = init, update = update, view = view }



-- Model


type alias Model =
    DataFrame


init : Model
init =
    "number,word,x\n1,one,1\n2,two,-1\n3,three,1" 
        |> CSV.fromCSV
        |> DF.andThen ( DF.join "rightanti" [ DF.col "number" ] ( CSV.fromCSV "number,x\n1,10\n2,20" ) )



-- Update


type Msg
    = None


update : Msg -> Model -> Model
update msg model =
    model



-- View


fieldHtml : Field -> Html Msg
fieldHtml field =
    text (Field.toString field)


rowHtml : List String -> Row -> Html Msg
rowHtml names row =
    names
        |> List.map
            (\name ->
                Maybe.withDefault
                    (UndefinedField ("Field " ++ name ++ " not found."))
                    (Dict.get name (Row.toDict row))
            )
        |> List.map (\field -> td [] [ fieldHtml field ])
        |> tr []


bodyHtml : DataFrame -> List (Html Msg)
bodyHtml df =
    let
        rows =
            DF.getRows df

        selected =
            DF.getSelected df
    in
    List.map (rowHtml selected) rows


headerHtml : DataFrame -> Html Msg
headerHtml df =
    df
        |> DF.getSelected
        |> List.map (\name -> th [] [ text name ])
        |> thead []


tableHtml : DataFrame -> Html Msg
tableHtml df =
    table []
        (headerHtml df :: bodyHtml df)


view : Model -> Html Msg
view model =
    let
        err = DF.getError model
    in
        case err of
            Nothing ->
                div [] [ tableHtml model ]

            Just error ->
                div [] [ text error ]
