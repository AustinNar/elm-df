module Field exposing (..)

type Field
  = NumericField Float
  | StringField String
  | UndefinedField String


type alias Calc a = a -> Field


isUndefined : Field -> Bool
isUndefined field = 
  case field of
    UndefinedField _ -> True
    _ -> False


toString : Field -> String
toString field =
  case field of
    NumericField float
      -> String.fromFloat float
    StringField string
      -> string
    UndefinedField reason
      -> reason


comp : Field -> Field -> Order
comp left right = 
  case ( left, right ) of
    ( NumericField leftNum, NumericField rightNum )
      -> compare leftNum rightNum
    ( StringField leftStr, StringField rightStr )
      -> compare leftStr rightStr
    _
      -> EQ