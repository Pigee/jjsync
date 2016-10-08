package main

import (
	//"bufio"
	"database/sql"
	"fmt"
	//"io/ioutil"
	//_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/alexbrainman/odbc"
	// "log"
	//"os"
	"time"
	// _ "github.com/go-sql-driver/mysql"
	// "jputil"
)

func main() {

	para1 := "ceshiceshi"
	para2 := "ceshiceshi"
	para3 := 75.4
	connjj, err := sql.Open("odbc", "dsn=jjwater;uid=sa;pwd=qyKe2852274")

	if err != nil {
		fmt.Println("%s:连接jjwater错误.%s\n", time.Now().Format("2006-01-02 15:04:05.000"), err)
		return
	}

	//stmt, err := connjj.Prepare("EXEC SP_TEST ?,?")
	//stmt, err := connjj.Prepare("INSERT INTO [FH_FGTLIST] ([FGTLIST_ID], [USERB_KH],[FGTFLOW_NO],[FGT_SFDATE]) SELECT NEWID(),?,NULL,GETDATE()")
	// union all select ""maxtime"",max(sync_date) from sfjl_sync where sync_date > ?")
	//connjj.Query("exec WX_DEBT_PAC ?,?,?", para1, para2, para3)

	//调用计费存储过程
	stmt_sp, err := connjj.Prepare("EXEC WX_DEBT_PAYC ?,?,?")

	if err != nil {
		fmt.Println("%s:Prepare SQL错误.%s\n", time.Now().Format("2006-01-02 15:04:05.000"), err)
		return
	}

	_, stmtRowsErr := stmt_sp.Query(para1, para2, para3) // , mx, mx)

	// stmt.Query("AB1234")

	if stmtRowsErr != nil {
		fmt.Printf("\nstmtRowsErr: %s", stmtRowsErr)
	}
	fmt.Println("过程完成\n")
	/*	row, err := stmt.Query("AB1234") // , mx, mx)
		if err != nil {
			fmt.Println("Query SQL错误:", err)
			return
		}

		for row.Next() {

			if err := row.Scan(&para1, &para2); err == nil {
				fmt.Printf("para1 is %s,and para2 is %d\n", para1, para2)
			}

		}



			file, err := os.Create("writeAt.txt")
			if err != nil {
				panic(err)
			}
			defer file.Close()
			n, err := file.WriteString("Golang中文社区——这里是多余的加了点东西")
			//file.Sync()

			// n, err := file.WriteAt([]byte("Go语言学习园地..别的东西"), 24)
			if err != nil {
				panic(err)
			}
			fmt.Println(n)

	*/
}
