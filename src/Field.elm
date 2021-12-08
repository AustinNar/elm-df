module Field exposing (..)

import Hash

type Field
    = NumericField Float
    | StringField String
    | BoolField Bool
    | NothingField
    | UndefinedField String


type alias Calc a =
    a -> Field


isUndefined : Field -> Bool
isUndefined field =
    case field of
        UndefinedField _ ->
            True

        _ ->
            False


toString : Field -> String
toString field =
    case field of
        NumericField float ->
            String.fromFloat float

        StringField string ->
            string

        BoolField bool ->
            case bool of
                True ->
                    "True"

                False ->
                    "False"

        NothingField -> 
            "Nothing"

        UndefinedField reason ->
            reason


comp : Field -> Field -> Order
comp left right =
    case ( left, right ) of
        ( NumericField leftNum, NumericField rightNum ) ->
            compare leftNum rightNum

        ( StringField leftStr, StringField rightStr ) ->
            compare leftStr rightStr

        ( BoolField leftBool, BoolField rightBool ) ->
            case ( leftBool, rightBool ) of
                ( True, False ) ->
                    GT

                ( False, True ) ->
                    LT

                _ ->
                    EQ

        ( NothingField, NothingField ) ->
            EQ

        ( UndefinedField leftStr, UndefinedField rightStr ) ->
            compare leftStr rightStr

        ( NumericField _, _ ) ->
            LT

        ( StringField _, NumericField _ ) ->
            GT

        ( BoolField _, NumericField _ ) ->
            GT

        ( NothingField, NumericField _ ) ->
            GT

        ( StringField _, _ ) ->
            LT

        ( BoolField _, StringField _ ) ->
            GT

        ( NothingField, StringField _ ) ->
            GT

        ( BoolField _, _ ) ->
            LT

        ( NothingField, BoolField _ ) ->
            GT

        ( NothingField, _) ->
            LT

        ( UndefinedField _, _ ) ->
            GT


compType : Field -> Field -> Order
compType left right =
    case ( left, right ) of
        ( NumericField _, NumericField _ ) ->
            EQ

        ( StringField _, StringField _ ) ->
            EQ

        ( UndefinedField _, UndefinedField _ ) ->
            EQ

        _ ->
            GT

hash : Field -> String
hash field = 
    case field of 
        NumericField num ->
            Hash.toString ( Hash.fromString ( "__NUMERIC__FIELD__" ++ String.fromFloat num ) )
        StringField str ->
            Hash.toString ( Hash.fromString ( "__STRING__FIELD__" ++ str ))
        BoolField bool ->
            if bool then
                Hash.toString ( Hash.fromString ( "__BOOL__FIELD__TRUE__" ) )
            else 
                Hash.toString ( Hash.fromString ( "__BOOL__FIELD__FALSE__" ) )
        NothingField ->
            Hash.toString ( Hash.fromString ( "__NOTHING_FIELD__" ) )
        UndefinedField str ->
            Hash.toString( Hash.fromString ( "__UNDEFINED__FIELD__" ++ str ) )