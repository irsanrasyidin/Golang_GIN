package repository

import (
	"database/sql"
	"fmt"
	"strconv"
	"Golang_GIN/model"
	"Golang_GIN/utils"
)

type PostgreSqlRepo interface {
	InsertPostgreSql(pstsql []*model.LogicModel) error
	GetPostgreSqlById(id string, page int) ([]*model.LogicModel, *model.Pagination, error)
	GetPostgreSqlByName(nama string, page int) ([]*model.LogicModel, *model.Pagination, error)
	GetPostgreSqlByEmail(email string, page int) ([]*model.LogicModel, *model.Pagination, error)
	GetPostgreSqlByGender(gender string, page int) ([]*model.LogicModel, *model.Pagination, error)
	GetAllPostgreSql(page int) ([]*model.LogicModel, *model.Pagination, error)
	EditPostgreSqlById(pstsql *model.LogicModel) error
	DeletePostgreSqlById(id string) error
	DeleteAllPostgreSql() error
	MainPostgreSqlExec(pstsql *model.ExecutionModel) error
}

type postgresqlRepoImpl struct {
	db *sql.DB
}

func (pstsqlRepo *postgresqlRepoImpl) InsertPostgreSql(pstsql []*model.LogicModel) error {
	pstsqlRepo.DeleteAllPostgreSql()
	query := "INSERT INTO data (Id, First_name, Last_name, Email, Gender, Avatar) VALUES "
	values := []interface{}{}

	for i := 0; i < len(pstsql); i++ {
		query += fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d),", i*6+1, i*6+2, i*6+3, i*6+4, i*6+5, i*6+6)
		id, _ := strconv.Atoi(pstsql[i].ID)
		values = append(values, id)
		values = append(values, pstsql[i].First_name)
		values = append(values, pstsql[i].Last_name)
		values = append(values, pstsql[i].Email)
		values = append(values, pstsql[i].Gender)
		values = append(values, pstsql[i].Avatar)
	}

	query = query[:len(query)-1]
	//fmt.Println(values...)
	//fmt.Println(query)

	_, err := pstsqlRepo.db.Exec(query, values...)
	if err != nil {
		return &utils.AppError{
			ErrorCode:    111,
			ErrorMessage: err.Error(),
		}
	}

	return nil
}

func (pstsqlRepo *postgresqlRepoImpl) GetPostgreSqlById(id string, page int) ([]*model.LogicModel, *model.Pagination, error) {
	var idInt int
	var lgc model.LogicModel
	var result []*model.LogicModel
	query := "SELECT ID, First_name, Last_name, Email, Gender, Avatar FROM data WHERE id=$1"
	err := pstsqlRepo.db.QueryRow(query, id).Scan(&idInt, &lgc.First_name, &lgc.Last_name, &lgc.Email, &lgc.Gender, &lgc.Avatar)
	if err != nil {
		return nil, nil, &utils.AppError{
			ErrorCode:    211,
			ErrorMessage: err.Error(),
		}
	}
	lgc.ID = strconv.Itoa(idInt)
	result = append(result, &lgc)

	return result, nil, nil
}

func (pstsqlRepo *postgresqlRepoImpl) GetPostgreSqlByName(nama string, page int) ([]*model.LogicModel, *model.Pagination, error) {
	query := "SELECT ID, First_name, Last_name, Email, Gender, Avatar FROM data WHERE First_name ILIKE '%' || $1 || '%' OR Last_name ILIKE '%' || $1 || '%'"
	rows, err := pstsqlRepo.db.Query(query, nama)
	if err != nil {
		return nil, nil, &utils.AppError{
			ErrorCode:    311,
			ErrorMessage: err.Error(),
		}
	}
	defer rows.Close()

	var result []*model.LogicModel

	for rows.Next() {
		var entry model.LogicModel
		err := rows.Scan(
			&entry.ID,
			&entry.First_name,
			&entry.Last_name,
			&entry.Email,
			&entry.Gender,
			&entry.Avatar,
		)
		if err != nil {
			return nil, nil, &utils.AppError{
				ErrorCode:    312,
				ErrorMessage: err.Error(),
			}
		}
		result = append(result, &entry)
	}

	if err := rows.Err(); err != nil {
		return nil, nil, &utils.AppError{
			ErrorCode:    313,
			ErrorMessage: err.Error(),
		}
	}
	return result, nil, nil
}

func (pstsqlRepo *postgresqlRepoImpl) GetPostgreSqlByEmail(email string, page int) ([]*model.LogicModel, *model.Pagination, error) {
	query := "SELECT ID, First_name, Last_name, Email, Gender, Avatar FROM data WHERE Email ILIKE '%' || $1 || '%'"
	rows, err := pstsqlRepo.db.Query(query, email)
	if err != nil {
		return nil, nil, &utils.AppError{
			ErrorCode:    411,
			ErrorMessage: err.Error(),
		}
	}
	defer rows.Close()

	var result []*model.LogicModel

	for rows.Next() {
		var entry model.LogicModel
		err := rows.Scan(
			&entry.ID,
			&entry.First_name,
			&entry.Last_name,
			&entry.Email,
			&entry.Gender,
			&entry.Avatar,
		)
		if err != nil {
			return nil, nil, &utils.AppError{
				ErrorCode:    412,
				ErrorMessage: err.Error(),
			}
		}
		result = append(result, &entry)
	}

	if err := rows.Err(); err != nil {
		return nil, nil, &utils.AppError{
			ErrorCode:    413,
			ErrorMessage: err.Error(),
		}
	}
	return result, nil, nil
}

func (pstsqlRepo *postgresqlRepoImpl) GetPostgreSqlByGender(gender string, page int) ([]*model.LogicModel, *model.Pagination, error) {
	query := "SELECT ID, First_name, Last_name, Email, Gender, Avatar FROM data WHERE gender =$1"
	rows, err := pstsqlRepo.db.Query(query, gender)
	if err != nil {
		return nil, nil, &utils.AppError{
			ErrorCode:    511,
			ErrorMessage: err.Error(),
		}
	}
	defer rows.Close()

	var result []*model.LogicModel

	for rows.Next() {
		var entry model.LogicModel
		err := rows.Scan(
			&entry.ID,
			&entry.First_name,
			&entry.Last_name,
			&entry.Email,
			&entry.Gender,
			&entry.Avatar,
		)
		if err != nil {
			return nil, nil, &utils.AppError{
				ErrorCode:    512,
				ErrorMessage: err.Error(),
			}
		}
		result = append(result, &entry)
	}

	if err := rows.Err(); err != nil {
		return nil, nil, &utils.AppError{
			ErrorCode:    513,
			ErrorMessage: err.Error(),
		}
	}
	return result, nil, nil
}

func (pstsqlRepo *postgresqlRepoImpl) GetAllPostgreSql(page int) ([]*model.LogicModel, *model.Pagination, error) {
	query := "SELECT ID, First_name, Last_name, Email, Gender, Avatar FROM data GROUP BY ID ORDER BY ID"
	rows, err := pstsqlRepo.db.Query(query)
	if err != nil {
		return nil, nil, &utils.AppError{
			ErrorCode:    612,
			ErrorMessage: err.Error(),
		}
	}
	defer rows.Close()
	var result []*model.LogicModel
	for rows.Next() {
		var entry model.LogicModel
		err := rows.Scan(
			&entry.ID,
			&entry.First_name,
			&entry.Last_name,
			&entry.Email,
			&entry.Gender,
			&entry.Avatar,
		)
		if err != nil {
			return nil, nil, &utils.AppError{
				ErrorCode:    613,
				ErrorMessage: err.Error(),
			}
		}

		result = append(result, &entry)
	}

	if err := rows.Err(); err != nil {
		return nil, nil, &utils.AppError{
			ErrorCode:    614,
			ErrorMessage: err.Error(),
		}
	}
	return result, nil, nil
}

func (pstsqlRepo *postgresqlRepoImpl) EditPostgreSqlById(pstsql *model.LogicModel) error {
	query := "UPDATE data SET First_name=$2, Last_name=$3, Email=$4, Gender=$5, Avatar=$6 WHERE ID=$1"

	// Eksekusi pernyataan UPDATE
	_, err := pstsqlRepo.db.Exec(query, pstsql.ID, pstsql.First_name, pstsql.Last_name, pstsql.Email, pstsql.Gender, pstsql.Avatar)
	if err != nil {
		return &utils.AppError{
			ErrorCode:    711,
			ErrorMessage: err.Error(),
		}
	}

	return nil
}

func (pstsqlRepo *postgresqlRepoImpl) DeletePostgreSqlById(id string) error {
	query := "DELETE FROM data WHERE ID=$1"

	_, err := pstsqlRepo.db.Exec(query, id)
	if err != nil {
		return &utils.AppError{
			ErrorCode:    811,
			ErrorMessage: err.Error(),
		}
	}

	return nil
}

func (pstsqlRepo *postgresqlRepoImpl) DeleteAllPostgreSql() error {
	query := "DELETE FROM data"

	_, err := pstsqlRepo.db.Exec(query)
	if err != nil {
		return &utils.AppError{
			ErrorCode:    911,
			ErrorMessage: err.Error(),
		}
	}

	return nil
}

func (pstsqlRepo *postgresqlRepoImpl) MainPostgreSqlExec(pstsql *model.ExecutionModel) error {
	existData, err := pstsqlRepo.GetPostgreSqlExec(pstsql.Nama)
	if err != nil {
		return &utils.AppError{
			ErrorCode:    1111,
			ErrorMessage: err.Error(),
		}
	}
	if existData == nil || len(existData) != 30 {
		pstsql.Coba = len(existData) + 1
		err = pstsqlRepo.InsertPostgreSqlExec(pstsql)
		if err != nil {
			return &utils.AppError{
				ErrorCode:    1112,
				ErrorMessage: err.Error(),
			}
		}
	} else {
		err = pstsqlRepo.DeletePostgreSqlExec(pstsql.Nama)
		if err != nil {
			return &utils.AppError{
				ErrorCode:    1113,
				ErrorMessage: err.Error(),
			}
		}
		err = pstsqlRepo.EditPostgreSqlExec(existData)
		if err != nil {
			return &utils.AppError{
				ErrorCode:    1114,
				ErrorMessage: err.Error(),
			}
		}
		pstsql.Coba = 30
		err = pstsqlRepo.InsertPostgreSqlExec(pstsql)
		if err != nil {
			return &utils.AppError{
				ErrorCode:    1115,
				ErrorMessage: err.Error(),
			}
		}
	}
	return nil
}

func (pstsqlRepo *postgresqlRepoImpl) InsertPostgreSqlExec(pstsql *model.ExecutionModel) error {
	query := "INSERT INTO execution (Nama,Masuk,Keluar,Duration,Coba) VALUES ($1,$2,$3,$4,$5)"

	tx, err := pstsqlRepo.db.Begin()
	if err != nil {
		return &utils.AppError{
			ErrorCode:    1110,
			ErrorMessage: err.Error(),
		}
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()

	stmt, err := tx.Prepare(query)
	if err != nil {
		return &utils.AppError{
			ErrorCode:    1111,
			ErrorMessage: err.Error(),
		}
	}
	defer stmt.Close()

	_, err = stmt.Exec(pstsql.Nama, pstsql.Masuk, pstsql.Keluar, pstsql.Duration, pstsql.Coba)
	if err != nil {
		return &utils.AppError{
			ErrorCode:    1112,
			ErrorMessage: err.Error(),
		}
	}

	return nil
}

func (pstsqlRepo *postgresqlRepoImpl) GetPostgreSqlExec(nama string) ([]*model.ExecutionModel, error) {
	query := "SELECT Nama,Masuk,Keluar,Duration,Coba FROM execution WHERE Nama=$1"
	rows, err := pstsqlRepo.db.Query(query, nama)
	if err != nil {
		return nil, &utils.AppError{
			ErrorCode:    1211,
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
				ErrorCode:    1212,
				ErrorMessage: err.Error(),
			}
		}
		result = append(result, &entry)
	}

	if err := rows.Err(); err != nil {
		return nil, &utils.AppError{
			ErrorCode:    1213,
			ErrorMessage: err.Error(),
		}
	}
	return result, nil
}

func (pstsqlRepo *postgresqlRepoImpl) EditPostgreSqlExec(pstsql []*model.ExecutionModel) error {
	fmt.Println(len(pstsql))
	query := "UPDATE execution SET Coba=$3 WHERE Nama=$1 AND Coba=$2"

	tx, err := pstsqlRepo.db.Begin()
	if err != nil {
		return &utils.AppError{
			ErrorCode:    1310,
			ErrorMessage: err.Error(),
		}
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()

	stmt, err := tx.Prepare(query)
	if err != nil {
		return &utils.AppError{
			ErrorCode:    1312,
			ErrorMessage: err.Error(),
		}
	}
	defer stmt.Close()

	for _, data := range pstsql {
		if data.Coba == 1 {
			continue
		}
		_, err = stmt.Exec(data.Nama, data.Coba, data.Coba-1)
		if err != nil {
			return &utils.AppError{
				ErrorCode:    1311,
				ErrorMessage: err.Error(),
			}
		}
	}

	return nil
}

func (pstsqlRepo *postgresqlRepoImpl) DeletePostgreSqlExec(nama string) error {
	query := "DELETE FROM execution Where Nama=$1 AND Coba=$2"

	_, err := pstsqlRepo.db.Exec(query, nama, 1)
	if err != nil {
		return &utils.AppError{
			ErrorCode:    1411,
			ErrorMessage: err.Error(),
		}
	}

	return nil
}

func (pstsqlRepo *postgresqlRepoImpl) DeleteAllPostgreSqlExec() error {
	query := "DELETE FROM execution"

	_, err := pstsqlRepo.db.Exec(query)
	if err != nil {
		return &utils.AppError{
			ErrorCode:    1411,
			ErrorMessage: err.Error(),
		}
	}

	return nil
}

func NewPostgreSqlRepo(db *sql.DB) PostgreSqlRepo {
	return &postgresqlRepoImpl{
		db: db,
	}
}
