package main

import (
	"database/sql"
	"fmt"
	_ "github.com/alexbrainman/odbc"
	//"io"
	"log"
	"os"
	"strconv"
	//"strings"
	"time"
	// _ "github.com/go-sql-driver/mysql"
	// "jputil"
	//"bufio"
	// "log"
	//"io/ioutil"
	//_ "github.com/denisenkom/go-mssqldb"
)

//main函数程序入口
func main() {

	// 链接jjtwater和jwater

	connjj, err := sql.Open("odbc", "dsn=jjdwater;uid=sa;pwd=qyKe2852274")

	if err != nil {
		fmt.Println("%s:连接jjtwater错误.%s\n", time.Now().Format("2006-01-02 15:04:05.000"), err)
		return
	}

	connj, err := sql.Open("odbc", "dsn=jdwater;uid=sa;pwd=2852274")

	if err != nil {
		fmt.Println("%s:连接jjtwater错误.%s\n", time.Now().Format("2006-01-02 15:04:05.000"), err)
		return
	}

	var wxTime string    //微信本次同步开始时间
	var wxmaxTime string //微信本次同步截止时间
	//var bkTime string    银行本次同步开始时间
	//var bkmaxTime string //银行本次同步截止时间
	cond := true
	for cond {

		wxTime = GetwxTime(connjj)
		//bkTime = GetbkTime(connjj)
		wxmaxTime = SetwxmaxTime(connj, connjj)
		//bkmaxTime = SetbkmaxTime(connj, connjj)
		if wxTime == wxmaxTime {
			fmt.Printf("%s:本次共同步0笔微信缴费数据...................\n", time.Now().Format("2006-01-02 15:04:05.000"))
		} else {
			wx_row := SyncwxRecords(wxTime, wxmaxTime, connj, connjj)
			fmt.Printf("%s:本次共同步%d笔微信缴费数据................\n", time.Now().Format("2006-01-02 15:04:05.000"), wx_row)
		}
		/*

			if bkTime == bkmaxTime {
				fmt.Printf("%s:本次共同步0笔银行缴费数据.................\n", time.Now().Format("2006-01-02 15:04:05.000"))
			} else {
				bk_row := SyncbkRecords(bkTime, bkmaxTime, connj, connjj)
				fmt.Printf("%s:本次共同步%d笔银行缴费数据...................\n", time.Now().Format("2006-01-02 15:04:05.000"), bk_row)
			}
		*/
		fmt.Printf("%s:-----------------------------------------------\n", time.Now().Format("2006-01-02 15:04:05.000"))
		time.Sleep(30 * time.Second)
		//cond = false
	}

	defer connjj.Close()
	defer connj.Close()
	return

}

//同步银行收费数据
func SyncbkRecords(bkTime string, bkmaxTime string, connj *sql.DB, connjj *sql.DB) int {

	stmt, err := connj.Prepare("select Yhdm, Lsh,  Kh, cast(left(Fyrq,4) as int),cast(right(Fyrq,2) as int),Fyrq, Dzrq, yjJe, sjJe, Wyj, Syys, Byys, Bz, CONVERT(VARCHAR(11),SFRQ,120)+SFSJ, kpbz from sfjl_sync where sync_date > ? and sync_date <= ?")
	// union all select ""maxtime"",max(sync_date) from sfjl_sync where sync_date > ?")

	if err != nil {
		fmt.Println("%s:Prepare bank SQL错误.%s\n", time.Now().Format("2006-01-02 15:04:05.000"), err)
		return 0
	}
	defer stmt.Close()

	row, err := stmt.Query(bkTime, bkmaxTime) // , mx, mx)
	if err != nil {
		fmt.Println("Query bank SQL错误:", err)
		return 0
	}
	var Yhdm string
	//var bkno string
	var Lsh string
	var Kh string
	var yea string
	var mon string
	var Fyrq string
	var Dzrq time.Time
	var yjJe float64
	var sjJe float64
	var Wyj float64
	var Syys float64
	var Byys float64
	var Bz string
	var Sfsj string
	var kpbz string
	r_count := 0
	for row.Next() {

		if err := row.Scan(&Yhdm, &Lsh, &Kh, &yea, &mon, &Fyrq, &Dzrq, &yjJe, &sjJe, &Wyj, &Syys, &Byys, &Bz, &Sfsj, &kpbz); err == nil {

			//fmt.Println(Yhdm, "--", yea, "--", mon, "NOrecords")
			//调用计费存储过程
			stmt_sp, err := connjj.Prepare("EXEC SP_BANK_SINGLEPAYC ?,?,?,?,?,?,?")

			if err != nil {
				fmt.Println("%s:Prepare SQL错误.%s\n", time.Now().Format("2006-01-02 15:04:05.000"), err)
				return 0
			}

			stmt_sp.Query(Kh, Yhdm, Lsh, Sfsj, sjJe+Wyj, yea, mon) // , mx, mx)

			stmt_sp.Close() //显式的关闭此连接，保证在存储过程调用执行完成后，才进入下一轮循环

			fmt.Printf("%s:同步卡号%s流水号%s账单金额%f完成.\n", time.Now().Format("2006-01-02 15:04:05.000"), Kh, Lsh, sjJe+Wyj)

			r_count++
		}
	}
	return r_count
}

//同步微信收费数据
func SyncwxRecords(wxTime string, wxmaxTime string, connj *sql.DB, connjj *sql.DB) int {

	//打开LOG文件

	stmt, err := connj.Prepare("select * from fh_weixindeal where deal_date > ? and deal_date <= ?")
	//union all select ""maxtime"",max(sync_date) from sfjl_sync where sync_date > ?")

	if err != nil {
		log.Println("Prepare weixin SQL错误.%s\n", err)
		return 0
	}

	row, err := stmt.Query(wxTime, wxmaxTime) // , mx, mx)
	if err != nil {
		log.Println("Query weixin SQL错误:%s", err)
		return 0
	}

	//初始化log资源
	logjj, _ := os.OpenFile("Jdinsertdata.log", os.O_CREATE|os.O_APPEND, 0)
	var logstr string
	var DEAL_ID string
	var DEAL_NO string
	var CUST_KH string
	var DEAL_MONEY float64
	var DEAL_DATE time.Time
	var CREATE_DATE time.Time
	var STATUS string
	var WX_NO string
	var SS_MONEY float64

	//var DT_WATERP_QAN int
	//var DT_USERB_SFFS string
	r_count := 0
	for row.Next() {

		if err := row.Scan(&DEAL_ID, &DEAL_NO, &CUST_KH, &DEAL_MONEY, &DEAL_DATE, &CREATE_DATE, &STATUS, &WX_NO, &SS_MONEY); err == nil {

			//初始化FH_WEIXINDEAL资源
			stmt, err := connjj.Prepare("insert into fh_weixindeal values (?,?,?,?,?,?,?,?,?)")

			if err != nil {
				fmt.Println("%s:Prepare SQL错误.%s\n", time.Now().Format("2006-01-02 15:04:05.000"), err)
				continue
			}
			defer stmt.Close()

			_, err = stmt.Query(DEAL_ID, DEAL_NO, CUST_KH, DEAL_MONEY, nil, CREATE_DATE, "0", nil, nil)

			logstr = time.Now().Format("2006-01-02 15:04:05.000") + ":插入DEAL卡:" + CUST_KH + "流水:" + DEAL_NO + "\r\n"
			fmt.Print(logstr)
			logjj.WriteString(logstr)

			//获得FH_WXDEALDETAIL数据
			var DT_DETAIL_ID string
			var DT_DEAL_NO string
			var DT_CUST_KH string
			var DT_DEAL_YEAR int
			var DT_DEAL_MONTH int
			var DT_DEAL_MONEY float64
			var DT_N_ZNJ float64

			stmt_dt, err := connj.Prepare("select [DETAIL_ID], [DEAL_NO], [CUST_KH], [DEAL_YEAR], [DEAL_MONTH], [DEAL_MONEY], [N_ZNJ] from  fh_wxdealdetail where deal_no = ?")

			if err != nil {
				log.Println("Prepare WXDEALDETAIL SQL错误.%s\n", err)
				return 0
			}

			row_dt, err := stmt_dt.Query(DEAL_NO) // , mx, mx)
			if err != nil {
				fmt.Println("Query WXDEALDETAIL  SQL错误:%s", err)
				return 0
			}

			for row_dt.Next() {

				if err := row_dt.Scan(&DT_DETAIL_ID, &DT_DEAL_NO, &DT_CUST_KH, &DT_DEAL_YEAR, &DT_DEAL_MONTH, &DT_DEAL_MONEY, &DT_N_ZNJ); err == nil {

					stmt_indt, err := connjj.Prepare("insert into fh_wxdealdetail([DETAIL_ID], [DEAL_NO], [CUST_KH], [DEAL_YEAR], [DEAL_MONTH], [DEAL_MONEY], [N_ZNJ]) values (?,?,?,?,?,?,?)")

					if err != nil {
						fmt.Println("%s:Prepare SQL错误.%s\n", time.Now().Format("2006-01-02 15:04:05.000"), err)
						return 0
					}

					stmt_indt.Query(DT_DETAIL_ID, DT_DEAL_NO, DT_CUST_KH, DT_DEAL_YEAR, DT_DEAL_MONTH, DT_DEAL_MONEY, DT_N_ZNJ)

					logstr = time.Now().Format("2006-01-02 15:04:05.000") + ":插入DETAIL:" + strconv.Itoa(DT_DEAL_YEAR) + "年" + strconv.Itoa(DT_DEAL_MONTH) + "月\r\n"
					fmt.Print(logstr)
					logjj.WriteString(logstr)

					stmt_indt.Close()

				}
			}

			stmt_sp, err := connjj.Prepare("EXEC WX_DEBT_PAYC ?,?,?")

			if err != nil {
				fmt.Println("%s:Prepare SQL错误.%s\n", time.Now().Format("2006-01-02 15:04:05.000"), err)
				return 0
			}

			_, err = stmt_sp.Exec(DEAL_NO, WX_NO, DEAL_MONEY)

			if err != nil {
				fmt.Println("%s:执行过程错误.%s\n", time.Now().Format("2006-01-02 15:04:05.000"), err)

			}

			fmt.Printf("%s:同步卡号%s流水号%s账单金额%f完成.\n", time.Now().Format("2006-01-02 15:04:05.000"), CUST_KH, DEAL_NO, DEAL_MONEY)

			stmt_sp.Close() //显式的关闭此连接，保证在存储过程调用执行完成后，才进入下一轮循
			stmt_dt.Close()

			//time.Sleep(time.Second)

		}
		r_count++

	}
	//stmt.Close()
	//	connj.Close()
	logjj.Close()
	return r_count
}

// 取得微信上次更新时间点
func GetwxTime(connjj *sql.DB) string {

	stmt, err := connjj.Prepare("select sync_date from sync_info where sync_name = ?")
	if err != nil {
		fmt.Println("%s:执行SQL错误.%s\n", time.Now().Format("2006-01-02 15:04:05.000"), err)
		return "未取得数据"
	}
	defer stmt.Close()

	row, err := stmt.Query("last_wxtime")
	if err != nil {
		fmt.Println("Query Error", err)
		return "未取得数据"
	}

	var jjTime time.Time
	for row.Next() {

		if err := row.Scan(&jjTime); err == nil {
			continue
			defer row.Close()

		}
	}
	//connjj.Close()

	defer stmt.Close()
	return jjTime.Format("2006-01-02 15:04:05.000")
}

// 设置下次次微信更新起始时间点，返回本次更新截止时间值
func SetwxmaxTime(connj *sql.DB, connjj *sql.DB) string {

	var maxTime time.Time

	stmt, err := connj.Prepare("select max(deal_date) from fh_weixindeal")
	if err != nil {
		fmt.Println("%s:执行SQL错误.%s\n", time.Now().Format("2006-01-02 15:04:05.000"), err)
		return "未取得数据"
	}
	defer stmt.Close()

	row, err := stmt.Query()
	if err != nil {
		fmt.Println("Query Error", err)
		return "未取得数据"
	}

	for row.Next() {

		if err := row.Scan(&maxTime); err == nil {
			continue
			defer row.Close()
		}

	}

	stmt_update, err := connjj.Prepare("update SYNC_INFO SET sync_date = ? where sync_name = ?")
	if err != nil {
		fmt.Println("%s:执行SQL错误.%s\n", time.Now().Format("2006-01-02 15:04:05.000"), err)
		return "未取得数据"
	}

	stmt_update.Query(maxTime, "last_wxtime")

	defer stmt_update.Close()
	defer stmt.Close()
	return maxTime.Format("2006-01-02 15:04:05.000")
}

// 取得银行上次更新时间点
func GetbkTime(connjj *sql.DB) string {

	stmt, err := connjj.Prepare("select sync_date from sync_info where sync_name = ?")
	if err != nil {
		fmt.Println("%s:执行SQL错误.%s\n", time.Now().Format("2006-01-02 15:04:05.000"), err)
		return "未取得数据"
	}
	defer stmt.Close()

	row, err := stmt.Query("last_bktime")
	if err != nil {
		fmt.Println("Query Error", err)
		return "未取得数据"
	}

	var jjTime time.Time
	for row.Next() {

		if err := row.Scan(&jjTime); err == nil {
			continue
			defer row.Close()

		}
	}
	//connjj.Close()
	return jjTime.Format("2006-01-02 15:04:05.000")
}

// 设置下次次银行更新起始时间点，返回本次更新截止时间值
func SetbkmaxTime(connj *sql.DB, connjj *sql.DB) string {

	var maxTime time.Time

	stmt, err := connj.Prepare("select max(sync_date) from sfjl_sync")
	if err != nil {
		fmt.Println("%s:执行SQL错误.%s\n", time.Now().Format("2006-01-02 15:04:05.000"), err)
		return "未取得数据"
	}
	defer stmt.Close()

	row, err := stmt.Query()
	if err != nil {
		fmt.Println("Query Error", err)
		return "未取得数据"
	}

	for row.Next() {

		if err := row.Scan(&maxTime); err == nil {
			continue
			defer row.Close()
		}

	}

	stmt_update, err := connjj.Prepare("update SYNC_INFO SET sync_date = ? where sync_name = ?")
	if err != nil {
		fmt.Println("%s:执行SQL错误.%s\n", time.Now().Format("2006-01-02 15:04:05.000"), err)
		return "未取得数据"
	}

	stmt_update.Query(maxTime, "last_bktime")

	defer stmt_update.Close()
	defer stmt.Close()
	return maxTime.Format("2006-01-02 15:04:05.000")

}

//func
//创建文件函数
func Createjjlog() {
	if Exist("insertjjData.log") == false {
		os.Create("insertjjData.log")
	}
}

// 检查文件或目录是否存在
// 如果由 filename 指定的文件或目录存在则返回 true，否则返回 false
func Exist(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil || os.IsExist(err)
}

/*
func insert(db *sql.DB) {
	stmt, err := db.Prepare("INSERT INTO user(username, password) VALUES(?, ?)")
	defer stmt.Close()

	if err != nil {
		log.Println(err)
		return
	}
	stmt.Exec("guotie", "guotie")
	stmt.Exec("testuser", "123123")

		// CreateFile()

	f, _ := os.OpenFile("maxtime.file", os.O_WRONLY|os.O_TRUNC, 0644)
	defer f.Close()
	n3, err := f.WriteString("TRUNCATE")

	if err != nil {
		panic(err)
	}

	fmt.Printf("wrote %d bytes\n", n3)
	f.Sync()
	//关闭文件和连接
	f.Close()

}

func main() {
	db, err := sql.Open("mysql", "fh_ssdd:fh_ssdd@tcp(120.33.89.2:3306)/fh_ssdd?charset=utf8")
	// db, err := sql.Open("mysql", "fh_xydd:fh_xydd@tcp(192.168.2.23:3306)/fh_xydd?charset=utf8")
	if err != nil {
		log.Fatalf("Open database error: %s\n", err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := db.Prepare("UPDATE t_static_iep set x022_day = ? where local_id = '7be8a047-59ab-40a2-a09e-4853e3cd6543' and date_time = ?")
	// stmt, err := db.Prepare("update t1 set id = 'that' where name = 'me'")
	defer stmt.Close()
	if err != nil {
		log.Println(err)
		return
	}

	if os.Args[1] == "help" {
		fmt.Println("function:update column x022_day of table t_static_iep manually")
		fmt.Println("Usage:fh_ssdd.exe x022_day_value 2015-09-08")
		return
	}
	if result, err := stmt.Exec(os.Args[1], os.Args[2]); err == nil {
		// if result, err := stmt.Exec(); err == nil {
		if c, err := result.RowsAffected(); err == nil {
			fmt.Println("update count : ", c)
		}
	}
	db.Close()
}

/////////////////////////////////////////
// maria_db.Close()
// postgresqldb parts
/*
	pgurl := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", "newdb", "newdb", "192.168.75.130", "5432", "newdb")
	tableowner := "pg_type"
	pg_db, err := sql.Open("postgres", pgurl)
	CheckErr(err)
	pg_rows, err := pg_db.Query("SELECT generate_create_table_statement($1)", tableowner)
	for pg_rows.Next() {
		var pgtb_name string
		err = pg_rows.Scan(&pgtb_name)
		CheckErr(err)
		fmt.Println(pgtb_name)
	}
	pg_db.Close()

}

func CheckErr(err error) {
	if err != nil {
		panic(err)
	}
}
*/
