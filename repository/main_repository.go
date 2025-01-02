package repository

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"Golang_GIN/model"
	"Golang_GIN/utils"
)

type MainRepo interface {
	MainPostgreSQL(option int) ([]*model.ExecutionModel, []*model.ExecutionResultModel, error)
	MainJSON(option int) ([]*model.ExecutionModel, []*model.ExecutionResultModel, error)
	Restart() error
}

type mainRepoImpl struct {
	db *sql.DB
}

func (mainRepo *mainRepoImpl) MainPostgreSQL(option int) ([]*model.ExecutionModel, []*model.ExecutionResultModel, error) {
	var a, b int
	mainData, err := mainRepo.GetAllPostgreSQL()
	if err != nil {
		return nil, nil, &utils.AppError{
			ErrorCode:    121,
			ErrorMessage: err.Error(),
		}
	}

	switch option {
	case 2:
		a = 0
		b = 1
	case 3:
		a = 1
		b = 6
	case 4:
		a = 6
		b = 7
	case 5:
		a = 7
		b = 9
	default:
		a = 0
		b = 9
	}
	resultData, newData, err := mainRepo.ProcessPostgreSQL(mainData, a, b)
	if err != nil {
		return nil, nil, &utils.AppError{
			ErrorCode:    221,
			ErrorMessage: err.Error(),
		}
	}
	if newData != nil {
		return newData, resultData, nil
	}
	//fmt.Println(resultData.Average)
	return mainData, resultData, nil
}

func (mainRepo *mainRepoImpl) GetAllPostgreSQL() ([]*model.ExecutionModel, error) {
	query := "SELECT nama,masuk, keluar, duration, coba FROM execution ORDER BY Nama, Coba"
	rows, err := mainRepo.db.Query(query)
	if err != nil {
		return nil, &utils.AppError{
			ErrorCode:    221,
			ErrorMessage: err.Error(),
		}
	}
	defer rows.Close()

	var result []*model.ExecutionModel

	for rows.Next() {
		var entry model.ExecutionModel
		err := rows.Scan(
			&entry.Nama,
			&entry.Masuk,
			&entry.Keluar,
			&entry.Duration,
			&entry.Coba,
		)
		if err != nil {
			return nil, &utils.AppError{
				ErrorCode:    222,
				ErrorMessage: err.Error(),
			}
		}
		result = append(result, &entry)
	}

	if err := rows.Err(); err != nil {
		return nil, &utils.AppError{
			ErrorCode:    223,
			ErrorMessage: err.Error(),
		}
	}
	return result, nil
}

func (mainRepo *mainRepoImpl) ProcessPostgreSQL(mainData []*model.ExecutionModel, a int, b int) ([]*model.ExecutionResultModel, []*model.ExecutionModel, error) {
	var resultName string
	var resultData []*model.ExecutionResultModel
	var newData []*model.ExecutionModel
	for i := a; i < b; i++ {
		switch i {
		case 0:
			resultName = "Insert"
			result, abc := mainRepo.ResultPostgreSQL(resultName, mainData)
			newData = append(newData, abc...)
			resultData = append(resultData, result)
		case 1:
			resultName = "GetByID"
			result, abc := mainRepo.ResultPostgreSQL(resultName, mainData)
			newData = append(newData, abc...)
			resultData = append(resultData, result)
		case 2:
			resultName = "GetByName"
			result, abc := mainRepo.ResultPostgreSQL(resultName, mainData)
			newData = append(newData, abc...)
			resultData = append(resultData, result)
		case 3:
			resultName = "GetByEmail"
			result, abc := mainRepo.ResultPostgreSQL(resultName, mainData)
			newData = append(newData, abc...)
			resultData = append(resultData, result)
		case 4:
			resultName = "GetByGender"
			result, abc := mainRepo.ResultPostgreSQL(resultName, mainData)
			newData = append(newData, abc...)
			resultData = append(resultData, result)
		case 5:
			resultName = "GetAll"
			result, abc := mainRepo.ResultPostgreSQL(resultName, mainData)
			newData = append(newData, abc...)
			resultData = append(resultData, result)
		case 6:
			resultName = "UpdateByID"
			result, abc := mainRepo.ResultPostgreSQL(resultName, mainData)
			newData = append(newData, abc...)
			resultData = append(resultData, result)
		case 7:
			resultName = "DeleteByID"
			result, abc := mainRepo.ResultPostgreSQL(resultName, mainData)
			newData = append(newData, abc...)
			resultData = append(resultData, result)
		case 8:
			resultName = "DeleteAll"
			result, abc := mainRepo.ResultPostgreSQL(resultName, mainData)
			newData = append(newData, abc...)
			resultData = append(resultData, result)
		}
	}
	if a != 0 || b != 9 {
		return resultData, newData, nil
	}
	return resultData, nil, nil
}

func (mainRepo *mainRepoImpl) ResultPostgreSQL(name string, mainData []*model.ExecutionModel) (*model.ExecutionResultModel, []*model.ExecutionModel) {
	var resultData model.ExecutionResultModel
	var newData []*model.ExecutionModel
	resultData.Nama = name
	//fmt.Println(name + "PostgreSQL")
	for _, value := range mainData {
		if value.Nama == name+"PostgreSQL" {
			if value.Duration < resultData.Top && resultData.Top != 0 {
				resultData.Top = value.Duration
			}
			if resultData.Top == 0 {
				resultData.Top = value.Duration
			}
			newData = append(newData, value)
			resultData.Data = append(resultData.Data, value.Duration)
		}
	}
	var deletedData float64
	for {
		resultData.S_Deviasi = utils.StandardDeviation(resultData.Data)
		if resultData.S_Deviasi > 10 {
			//fmt.Println("Data sebelum di filter ", resultData.Data)
			resultData.Data, resultData.Outliner, deletedData = utils.Filter(resultData.Data, resultData.Outliner)
			resultData.OutlinerData = append(resultData.OutlinerData, deletedData)
			//fmt.Println("Data setelah di filter ", resultData.Data)
		} else {
			//fmt.Println("Data ", resultData.Data)
			break
		}
	}

	resultData.Average = utils.AverageDuration(resultData.Data)
	//fmt.Println("standar deviasi", resultData.S_Deviasi)
	//fmt.Println("Jumlah Outliner", resultData.Outliner)
	//fmt.Println("Data Outliner", resultData.OutlinerData)
	//fmt.Println(resultData)
	return &resultData, newData
}

func (mainRepo *mainRepoImpl) MainJSON(option int) ([]*model.ExecutionModel, []*model.ExecutionResultModel, error) {
	var a, b int
	mainData, err := mainRepo.GetAllJSON()
	if err != nil {
		return nil, nil, &utils.AppError{
			ErrorCode:    121,
			ErrorMessage: err.Error(),
		}
	}
	switch option {
	case 2:
		a = 0
		b = 1
	case 3:
		a = 1
		b = 6
	case 4:
		a = 6
		b = 7
	case 5:
		a = 7
		b = 9
	default:
		a = 0
		b = 9
	}
	resultData, newData, err := mainRepo.ProcessJSON(mainData, a, b)
	if err != nil {
		return nil, nil, &utils.AppError{
			ErrorCode:    221,
			ErrorMessage: err.Error(),
		}
	}
	if newData != nil {
		return newData, resultData, nil
	}
	return nil, resultData, nil
}

func (mainRepo *mainRepoImpl) GetAllJSON() ([]*model.ExecutionModel, error) {
	jsonFile, err := os.Open("execution.json")
	if err != nil {
		return nil, &utils.AppError{
			ErrorCode:    601,
			ErrorMessage: err.Error(),
		}
	}
	defer jsonFile.Close()

	byteValue, _ := io.ReadAll(jsonFile)
	if err != nil {
		return nil, &utils.AppError{
			ErrorCode:    602,
			ErrorMessage: err.Error(),
		}
	}

	var jsonData []model.ExecutionModel
	var limitData []*model.ExecutionModel
	json.Unmarshal(byteValue, &jsonData)
	for i := 0; i < len(jsonData); i++ {
		limitData = append(limitData, &jsonData[i])
	}
	return limitData, nil
}

func (mainRepo *mainRepoImpl) ProcessJSON(mainData []*model.ExecutionModel, a int, b int) ([]*model.ExecutionResultModel, []*model.ExecutionModel, error) {
	var resultName string
	var resultData []*model.ExecutionResultModel
	var newData []*model.ExecutionModel
	for i := a; i < b; i++ {
		switch i {
		case 0:
			resultName = "Insert"
			result, abc := mainRepo.ResultJSON(resultName, mainData)
			newData = append(newData, abc...)
			resultData = append(resultData, result)
		case 1:
			resultName = "GetByID"
			result, abc := mainRepo.ResultJSON(resultName, mainData)
			newData = append(newData, abc...)
			resultData = append(resultData, result)
		case 2:
			resultName = "GetByName"
			result, abc := mainRepo.ResultJSON(resultName, mainData)
			newData = append(newData, abc...)
			resultData = append(resultData, result)
		case 3:
			resultName = "GetByEmail"
			result, abc := mainRepo.ResultJSON(resultName, mainData)
			newData = append(newData, abc...)
			resultData = append(resultData, result)
		case 4:
			resultName = "GetByGender"
			result, abc := mainRepo.ResultJSON(resultName, mainData)
			newData = append(newData, abc...)
			resultData = append(resultData, result)
		case 5:
			resultName = "GetAll"
			result, abc := mainRepo.ResultJSON(resultName, mainData)
			newData = append(newData, abc...)
			resultData = append(resultData, result)
		case 6:
			resultName = "UpdateByID"
			result, abc := mainRepo.ResultJSON(resultName, mainData)
			newData = append(newData, abc...)
			resultData = append(resultData, result)
		case 7:
			resultName = "DeleteByID"
			result, abc := mainRepo.ResultJSON(resultName, mainData)
			newData = append(newData, abc...)
			resultData = append(resultData, result)
		case 8:
			resultName = "DeleteAll"
			result, abc := mainRepo.ResultJSON(resultName, mainData)
			newData = append(newData, abc...)
			resultData = append(resultData, result)
		}
	}
	if a != 0 || b != 9 {
		return resultData, newData, nil
	}
	return resultData, nil, nil
}

func (mainRepo *mainRepoImpl) ResultJSON(name string, mainData []*model.ExecutionModel) (*model.ExecutionResultModel, []*model.ExecutionModel) {
	var resultData model.ExecutionResultModel
	var newData []*model.ExecutionModel
	resultData.Nama = name
	//fmt.Println(name + "JSON")
	for _, value := range mainData {
		if value.Nama == name+"JSON" {
			if resultData.Top == 0 {
				resultData.Top = value.Duration
			}
			if value.Duration < resultData.Top {
				resultData.Top = value.Duration
			}
			newData = append(newData, value)
			resultData.Data = append(resultData.Data, value.Duration)
		}
	}

	var deletedData float64
	for {
		resultData.S_Deviasi = utils.StandardDeviation(resultData.Data)
		if resultData.S_Deviasi > 10 {
			//fmt.Println("Data sebelum di filter ", resultData.Data)
			resultData.Data, resultData.Outliner, deletedData = utils.Filter(resultData.Data, resultData.Outliner)
			resultData.OutlinerData = append(resultData.OutlinerData, deletedData)
			//fmt.Println("Data setelah di filter ", resultData.Data)
		} else {
			//fmt.Println("Data ", resultData.Data)
			break
		}
	}

	resultData.Average = utils.AverageDuration(resultData.Data)
	//fmt.Println("standar deviasi", resultData.S_Deviasi)
	//fmt.Println("Jumlah Outliner", resultData.Outliner)
	//fmt.Println("Data Outliner", resultData.OutlinerData)
	//fmt.Println(resultData)
	return &resultData, newData
}

func (mainRepo *mainRepoImpl) Restart() error {
	mainData, err := mainRepo.GetAllPostgreSQL()
	if err != nil {
		return &utils.AppError{
			ErrorCode:    701,
			ErrorMessage: err.Error(),
		}
	}

	var count int
	query := "SELECT COUNT(*) FROM data"
	err = mainRepo.db.QueryRow(query).Scan(&count)
	if err != nil {
		return &utils.AppError{
			ErrorCode:    705,
			ErrorMessage: err.Error(),
		}
	}

	err = SaveFile(mainData, count)
	if err != nil {
		return &utils.AppError{
			ErrorCode:    702,
			ErrorMessage: err.Error(),
		}
	}

	// jsonFile, err := os.Create("execution.json")
	// if err != nil {
	// 	return &utils.AppError{
	// 		ErrorCode:    703,
	// 		ErrorMessage: err.Error(),
	// 	}
	// }
	// defer jsonFile.Close()

	// encoder := json.NewEncoder(jsonFile)
	// err = encoder.Encode("[]")
	// if err != nil {
	// 	return &utils.AppError{
	// 		ErrorCode:    704,
	// 		ErrorMessage: err.Error(),
	// 	}
	// }

	// jsonFile2, err := os.Create("output.json")
	// if err != nil {
	// 	return &utils.AppError{
	// 		ErrorCode:    703,
	// 		ErrorMessage: err.Error(),
	// 	}
	// }
	// defer jsonFile2.Close()

	// encoder = json.NewEncoder(jsonFile2)
	// err = encoder.Encode("[]")
	// if err != nil {
	// 	return &utils.AppError{
	// 		ErrorCode:    704,
	// 		ErrorMessage: err.Error(),
	// 	}
	// }

	// query = "DELETE FROM execution"
	// _, err = mainRepo.db.Exec(query)
	// if err != nil {
	// 	return &utils.AppError{
	// 		ErrorCode:    705,
	// 		ErrorMessage: err.Error(),
	// 	}
	// }

	// query = "DELETE FROM data"
	// _, err = mainRepo.db.Exec(query)
	// if err != nil {
	// 	return &utils.AppError{
	// 		ErrorCode:    705,
	// 		ErrorMessage: err.Error(),
	// 	}
	// }
	return nil
}

func SaveFile(mainData []*model.ExecutionModel, count int) error {

	namaFile := fmt.Sprintf("data_%d.csv", count)

	file, err := os.Create(namaFile)
	if err != nil {
		return &utils.AppError{
			ErrorCode:    801,
			ErrorMessage: err.Error(),
		}
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	header := []string{"Nama", "Masuk", "Keluar", "Duration", "Coba"}
	writer.Write(header)

	for _, record := range mainData {
		durationString := fmt.Sprintf("%.3f", record.Duration)
		cobaString := strconv.Itoa(record.Coba)

		recordSlice := []string{
			record.Nama,
			record.Masuk,
			record.Keluar,
			durationString,
			cobaString,
		}

		if err := writer.Write(recordSlice); err != nil {
			return &utils.AppError{
				ErrorCode:    803,
				ErrorMessage: err.Error(),
			}
		}
	}
	return nil
}

func NewMainRepo(db *sql.DB) MainRepo {
	return &mainRepoImpl{
		db: db,
	}
}
