package model

type LogicModel struct {
	ID         string `json:"ID"`
	First_name string `json:"First_name"`
	Last_name  string `json:"Last_name"`
	Email      string `json:"Email"`
	Gender     string `json:"Gender"`
	Avatar     string `json:"Avatar"`
}

type Pagination struct {
	Page       int    `json:"Page"`
	PageType   string `json:"Page_Type"`
	PageSize   int    `json:"Page_Size"`
	TotalItems int    `json:"Total_Items"`
}

type ExecutionModel struct {
	Nama     string  `json:"Nama"`
	Masuk    string  `json:"Masuk"`
	Keluar   string  `json:"Keluar"`
	Duration float64 `json:"Duration"`
	Coba     int     `json:"Coba"`
	Status   string  `json:"Status"`
}

type ExecutionResultModel struct {
	Nama         string    `json:"Nama"`
	Average      float64   `json:"Average"`
	Top          float64   `json:"Top"`
	Data         []float64 `json:"Data"`
	S_Deviasi    float64   `json:"S_Deviasi"`
	Outliner     int       `json:"Outliner"`
	OutlinerData []float64 `json:"OutlinerData"`
}
