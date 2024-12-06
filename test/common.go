package test

import (
	"archive/zip"
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/liminaab/filtrify/types"
)

var uat1TestData [][]string = [][]string{
	{"Instrument name", "Instrument Type", "Quantity", "Market Value (Base)", "Exposure %", "Maturity Date", "EU Sanction listed", "Active From"},
	{"ERIC B SS Equity", "Equity", "175 000.00", "2 000 000.00", "8%", "", "true", "2020-01-01 12:00:00"},
	{"AMZN US Equity", "Equity", "1 500.00", "6 000 000.00", "25%", "", "false", "2020-03-01 12:00:00"},
	{"T 0 12/31/21", "Bill", "9 000 000.00", "8 750 000.00", "30%", "2021-12-31", "false", "2020-11-22 12:00:00"},
	{"ESZ1", "Index Future", "-10.00", "-495 000.00", "17%", "2021-12-16", "false", "2021-04-06 12:00:00"},
	{"USD Cash", "Cash Account", "5 000 000.00", "5 000 000.0", "20%", "", "", "2020-01-01 12:00:00"},
}

var UAT1TestDataFormatted [][]string = [][]string{
	{"Instrument name", "Instrument Type", "Quantity", "Market Value (Base)", "Exposure %", "Maturity Date", "EU Sanction listed", "Active From"},
	{"ERIC B SS Equity", "Equity", "175000.00", "2000000.00", "8%", "", "true", "2020-01-01 12:00:00"},
	{"AMZN US Equity", "Equity", "1500.00", "6000000.00", "25%", "", "false", "2020-03-01 12:00:00"},
	{"T 0 12/31/21", "Bill", "9000000.00", "8750000.00", "30%", "2021-12-31", "false", "2020-11-22 12:00:00"},
	{"ESZ1", "Index Future", "-10.00", "-495000.00", "17%", "2021-12-16", "false", "2021-04-06 12:00:00"},
	{"USD Cash", "Cash Account", "5000000.00", "5000000.0", "20%", "", "", "2020-01-01 12:00:00"},
}

var UAT1TestDataSet *types.DataSet = &types.DataSet{
	Headers: types.HeaderMap{
		"Instrument name": &types.Header{
			ColumnName: "Instrument name",
			DataType:   types.StringType,
		},
		"Instrument Type": &types.Header{
			ColumnName: "Instrument Type",
			DataType:   types.StringType,
		},
		"Quantity": &types.Header{
			ColumnName: "Quantity",
			DataType:   types.DoubleType,
		},
		"Market Value (Base)": &types.Header{
			ColumnName: "Market Value (Base)",
			DataType:   types.DoubleType,
		},
		"Exposure %": &types.Header{
			ColumnName: "Exposure %",
			DataType:   types.StringType,
		},
		"Maturity Date": &types.Header{
			ColumnName: "Maturity Date",
			DataType:   types.DateType,
		},
		"EU Sanction listed": &types.Header{
			ColumnName: "EU Sanction listed",
			DataType:   types.BoolType,
		},
		"Active From": &types.Header{
			ColumnName: "Active From",
			DataType:   types.TimestampType,
		},
	},
	Rows: []*types.DataRow{
		{
			Key: nil,
			Columns: []*types.DataColumn{
				{
					ColumnName: "Instrument name",
					CellValue: &types.CellValue{
						DataType:    types.StringType,
						StringValue: "ERIC B SS Equity",
					},
				},
				{
					ColumnName: "Instrument Type",
					CellValue: &types.CellValue{
						DataType:    types.StringType,
						StringValue: "Equity",
					},
				},
				{
					ColumnName: "Quantity",
					CellValue: &types.CellValue{
						DataType:    types.DoubleType,
						DoubleValue: 175000.00,
					},
				},
				{
					ColumnName: "Market Value (Base)",
					CellValue: &types.CellValue{
						DataType:    types.DoubleType,
						DoubleValue: 2000000.00,
					},
				},
				{
					ColumnName: "Exposure %",
					CellValue: &types.CellValue{
						DataType:    types.StringType,
						StringValue: "8%",
					},
				},
				{
					ColumnName: "Maturity Date",
					CellValue: &types.CellValue{
						DataType: types.NilType,
					},
				},
				{
					ColumnName: "EU Sanction listed",
					CellValue: &types.CellValue{
						DataType:  types.BoolType,
						BoolValue: true,
					},
				},
				{
					ColumnName: "Active From",
					CellValue: &types.CellValue{
						DataType:       types.TimestampType,
						TimestampValue: time.Date(2020, 01, 01, 12, 00, 00, 0, time.UTC),
					},
				},
			},
		},
		{
			Key: nil,
			Columns: []*types.DataColumn{
				{
					ColumnName: "Instrument name",
					CellValue: &types.CellValue{
						DataType:    types.StringType,
						StringValue: "AMZN US Equity",
					},
				},
				{
					ColumnName: "Instrument Type",
					CellValue: &types.CellValue{
						DataType:    types.StringType,
						StringValue: "Equity",
					},
				},
				{
					ColumnName: "Quantity",
					CellValue: &types.CellValue{
						DataType:    types.DoubleType,
						DoubleValue: 1500.00,
					},
				},
				{
					ColumnName: "Market Value (Base)",
					CellValue: &types.CellValue{
						DataType:    types.DoubleType,
						DoubleValue: 6000000.00,
					},
				},
				{
					ColumnName: "Exposure %",
					CellValue: &types.CellValue{
						DataType:    types.StringType,
						StringValue: "25%",
					},
				},
				{
					ColumnName: "Maturity Date",
					CellValue: &types.CellValue{
						DataType: types.NilType,
					},
				},
				{
					ColumnName: "EU Sanction listed",
					CellValue: &types.CellValue{
						DataType:  types.BoolType,
						BoolValue: false,
					},
				},
				{
					ColumnName: "Active From",
					CellValue: &types.CellValue{
						DataType:       types.TimestampType,
						TimestampValue: time.Date(2020, 03, 01, 12, 00, 00, 0, time.UTC),
					},
				},
			},
		},
		{
			Key: nil,
			Columns: []*types.DataColumn{
				{
					ColumnName: "Instrument name",
					CellValue: &types.CellValue{
						DataType:    types.StringType,
						StringValue: "T 0 12/31/21",
					},
				},
				{
					ColumnName: "Instrument Type",
					CellValue: &types.CellValue{
						DataType:    types.StringType,
						StringValue: "Bill",
					},
				},
				{
					ColumnName: "Quantity",
					CellValue: &types.CellValue{
						DataType:    types.DoubleType,
						DoubleValue: 9000000.00,
					},
				},
				{
					ColumnName: "Market Value (Base)",
					CellValue: &types.CellValue{
						DataType:    types.DoubleType,
						DoubleValue: 8750000.00,
					},
				},
				{
					ColumnName: "Exposure %",
					CellValue: &types.CellValue{
						DataType:    types.StringType,
						StringValue: "30%",
					},
				},
				{
					ColumnName: "Maturity Date",
					CellValue: &types.CellValue{
						DataType:       types.DateType,
						TimestampValue: time.Date(2021, 12, 31, 0, 0, 0, 0, time.UTC),
					},
				},
				{
					ColumnName: "EU Sanction listed",
					CellValue: &types.CellValue{
						DataType:  types.BoolType,
						BoolValue: false,
					},
				},
				{
					ColumnName: "Active From",
					CellValue: &types.CellValue{
						DataType:       types.TimestampType,
						TimestampValue: time.Date(2020, 11, 22, 12, 00, 00, 0, time.UTC),
					},
				},
			},
		},
		{
			Key: nil,
			Columns: []*types.DataColumn{
				{
					ColumnName: "Instrument name",
					CellValue: &types.CellValue{
						DataType:    types.StringType,
						StringValue: "ESZ1",
					},
				},
				{
					ColumnName: "Instrument Type",
					CellValue: &types.CellValue{
						DataType:    types.StringType,
						StringValue: "Index Future",
					},
				},
				{
					ColumnName: "Quantity",
					CellValue: &types.CellValue{
						DataType:    types.DoubleType,
						DoubleValue: -10.00,
					},
				},
				{
					ColumnName: "Market Value (Base)",
					CellValue: &types.CellValue{
						DataType:    types.DoubleType,
						DoubleValue: -495000.00,
					},
				},
				{
					ColumnName: "Exposure %",
					CellValue: &types.CellValue{
						DataType:    types.StringType,
						StringValue: "17%",
					},
				},
				{
					ColumnName: "Maturity Date",
					CellValue: &types.CellValue{
						DataType:       types.DateType,
						TimestampValue: time.Date(2021, 12, 16, 0, 0, 0, 0, time.UTC),
					},
				},
				{
					ColumnName: "EU Sanction listed",
					CellValue: &types.CellValue{
						DataType:  types.BoolType,
						BoolValue: false,
					},
				},
				{
					ColumnName: "Active From",
					CellValue: &types.CellValue{
						DataType:       types.TimestampType,
						TimestampValue: time.Date(2021, 04, 06, 12, 00, 00, 0, time.UTC),
					},
				},
			},
		},
		{
			Key: nil,
			Columns: []*types.DataColumn{
				{
					ColumnName: "Instrument name",
					CellValue: &types.CellValue{
						DataType:    types.StringType,
						StringValue: "USD Cash",
					},
				},
				{
					ColumnName: "Instrument Type",
					CellValue: &types.CellValue{
						DataType:    types.StringType,
						StringValue: "Cash Account",
					},
				},
				{
					ColumnName: "Quantity",
					CellValue: &types.CellValue{
						DataType:    types.DoubleType,
						DoubleValue: 5000000.00,
					},
				},
				{
					ColumnName: "Market Value (Base)",
					CellValue: &types.CellValue{
						DataType:    types.DoubleType,
						DoubleValue: 5000000.00,
					},
				},
				{
					ColumnName: "Exposure %",
					CellValue: &types.CellValue{
						DataType:    types.StringType,
						StringValue: "20%",
					},
				},
				{
					ColumnName: "Maturity Date",
					CellValue: &types.CellValue{
						DataType: types.NilType,
					},
				},
				{
					ColumnName: "EU Sanction listed",
					CellValue: &types.CellValue{
						DataType: types.NilType,
					},
				},
				{
					ColumnName: "Active From",
					CellValue: &types.CellValue{
						DataType:       types.TimestampType,
						TimestampValue: time.Date(2020, 01, 01, 12, 00, 00, 0, time.UTC),
					},
				},
			},
		},
	},
}

var UAT1TestDataFormattedWithDate [][]string = [][]string{
	{"Instrument name", "Instrument Type", "Quantity", "Market Value (Base)", "Exposure %", "Maturity Date", "EU Sanction listed", "Active From"},
	{"ERIC B SS Equity", "Equity", "175000.00", "2000000.00", "8%", "", "true", "2020-01-01"},
	{"AMZN US Equity", "Equity", "1500.00", "6000000.00", "25%", "", "false", "2020-03-01"},
	{"T 0 12/31/21", "Bill", "9000000.00", "8750000.00", "30%", "2021-12-31", "false", "2020-11-22"},
	{"ESZ1", "Index Future", "-10.00", "-495000.00", "17%", "2021-12-16", "false", "2021-04-06"},
	{"USD Cash", "Cash Account", "5000000.00", "5000000.0", "20%", "", "", "2020-01-01"},
}

var UAT1TestDataFormattedWithTime [][]string = [][]string{
	{"Instrument name", "Instrument Type", "Quantity", "Market Value (Base)", "Exposure %", "Maturity Date", "EU Sanction listed", "Active From"},
	{"ERIC B SS Equity", "Equity", "175000.00", "2000000.00", "8%", "", "true", "12:00:00"},
	{"AMZN US Equity", "Equity", "1500.00", "6000000.00", "25%", "", "false", "16:00:00"},
	{"T 0 12/31/21", "Bill", "9000000.00", "8750000.00", "30%", "2021-12-31", "false", "08:00:00"},
	{"ESZ1", "Index Future", "-10.00", "-495000.00", "17%", "2021-12-16", "false", "08:13:00"},
	{"USD Cash", "Cash Account", "5000000.00", "5000000.0", "20%", "", "", "13:40:00"},
}

var UAT2TestDataFormatted [][]string = [][]string{
	{"Instrument name", "Instrument Type", "Quantity", "Market Value (Base)", "Exposure %", "Maturity Date", "EU Sanction listed", "Active From"},
	{" ERIC B SS Equity ", "Equity", "1.00", "2000000.00", "8%", "", "true", "2020-01-01 12:00:00"},
	{"AMZN US Equity", "Equity", "2.00", "6000000.00", "25%", "", "false", "2020-03-01 12:00:00"},
	{"T 0 12/31/21", "Bill", "3.00", "8750000.00", "30%", "2021-12-31", "false", "2020-11-22 12:00:00"},
	{"ESZ1", "Index Future", "-10.00", "-495000.00", "17%", "2021-12-16", "false", "2021-04-06 12:00:00"},
	{"ESZ2", "Cash Account2", "", "5000000.0", "20%", "", "", "2020-01-01 12:00:00"},
	{"true", "Cash Account", "4.00", "5000000.0", "20%", "", "", "2020-01-01 12:00:00"},
}

var UAT3TestDataFormatted [][]string = [][]string{
	{"Instrument name", "Instrument Type", "Quantity", "Market Value (Base)", "Exposure %", "Maturity Date", "EU Sanction listed", "Active From", "Hour", "somedata"},
	{"a", "Equity", "1.00", "2000000.00", "8%", "2020-12-31", "true", "2020-01-01 12:00:00", "08:00:00", "1"},
	{"b", "Equity", "2.00", "6000000.00", "25%", "", "false", "2020-03-01 12:00:00", "09:00:00", "2"},
	{"e", "Bill", "3.00", "8750000.00", "30%", "2022-12-31", "false", "2020-11-22 12:00:00", "10:00:00", "3"},
	{"", "Index Future", "-10.00", "-495000.00", "17%", "2021-12-16", "false", "2021-04-06 12:00:00", "11:00:00", "4"},
	{"d", "Cash Account2", "2.60", "5000000.0", "20%", "", "", "2020-02-01 12:00:00", "", "5"},
	{"c", "Cash Account", "4.00", "5000000.0", "20%", "", "", "2020-05-01 12:00:00", "12:00:00", ""},
}

var UATAggregateTestDataFormatted [][]string = [][]string{
	{"Instrument name", "Instrument Type", "Quantity", "Market Value (Base)", "Exposure %", "Maturity Date", "EU Sanction listed", "Active From", "Currency"},
	{"ERIC B SS Equity", "Equity", "175000.00", "2000000.00", "8%", "", "true", "2020-01-01 12:00:00", "SEK"},
	{"AMZN US Equity", "Equity", "1500.00", "6000000.00", "25%", "", "true", "2020-03-01 12:00:00", "USD"},
	{"T 0 12/31/21", "Bill", "9000000.00", "8750000.00", "30%", "2021-12-31", "false", "2020-11-22 12:00:00", "USD"},
	{"ESZ1", "Index Future", "-10.00", "-495000.00", "17%", "2021-12-16", "false", "2021-04-06 12:00:00", "USD"},
	{"USD Cash", "Cash Account", "5000000.00", "5000000.0", "20%", "", "", "2020-01-01 12:00:00", "USD"},
}

var UATLookupTestDataFormatted [][]string = [][]string{
	{"Instrument name", "Instrument ID", "Quantity", "ISIN", "Currency"},
	{"ERIC B SS Equity", "1", "175 000.00", "SE0000108656", "SEK"},
	{"AMZN US Equity", "2", "1 500.00", "US0231351067", "USD"},
	{"T 0 12/31/21", "3", "9 000 000.00", "US0231399991", "USD"},
	{"ESZ1", "4", "-10.00", "", "USD"},
	{"USD Cash", "", "5 000 000.00", "", "USD"},
	{"ERIC B LN Equity", "5", "175 000.00", "SE0000108656", "CHF"},
}

var UATLookupJoinTestDataFormatted [][]string = [][]string{
	{"Instrument ID", "Instrument name", "ISIN", "Currency", "Region"},
	{"1", "ERIC B SS Equity", "SE0000108656", "SEK", "Europe"},
	{"2", "AMZN US Equity", "US0231351067", "USD", "Americas"},
	{"3", "T 0 12/31/21", "US0231399991", "USD", "Americas"},
	{"4", "ESZ1", "", "USD", "Americas"},
	{"5", "ERIC B LN Equity", "SE0000108656", "CHF", "Europe"},
}

var UATMappedValueTestDataFormatted [][]string = [][]string{
	{"Instrument name", "Broker ID", "Quantity"},
	{"ERIC B SS Equity", "1", "175 000.00"},
	{"AMZN US Equity", "2", "1 500.00"},
	{"T 0 12/31/21", "1", "9 000 000.00"},
	{"ESZ1", "1", "-10.00"},
	{"USD Cash", "", "5 000 000.00"},
	{"ERIC B LN Equity", "3", "175 000.00"},
}

var UATMappedValueMapTestDataFormatted [][]string = [][]string{
	{"Key", "Value"},
	{"1", "Goldman Sachs Int."},
	{"2", "UBS"},
	{"3", "Credit Suisse"},
	{"4", "SEB"},
}

var UATMappedValueMapEmbeddedTestDataFormatted [][]string = [][]string{
	{"1", "Goldman Sachs Int."},
	{"2", "UBS"},
	{"3", "Credit Suisse"},
	{"4", "SEB"},
}

var TestDataWithFields = [][]string{
	{"user_id", "username", "email", "age", "gender", "country", "job_title", "salary", "join_date", "active_status"},
	{"1", "bklimkov0", "mconklin0@oracle.com", "86", "Female", "China", "Marketing Manager", "87587.58", "6/7/2022", "true"},
	{"2", "rblabey1", "etratton1@opensource.org", "100", "Non-binary", "Indonesia", "Civil Engineer", "43542.72", "1/22/2018", "false"},
	{"3", "wdeville2", "akarolczyk2@reverbnation.com", "63", "Female", "Germany", "Nurse Practicioner", "145078.84", "6/15/2018", "true"},
	{"4", "ckedge3", "hkelwick3@paypal.com", "45", "Female", "Indonesia", "Software Test Engineer II", "94372.67", "6/10/2015", "true"},
	{"5", "mjimes4", "gtarborn4@accuweather.com", "29", "Female", "Tunisia", "Nurse", "82711.45", "5/4/2017", "false"},
	{"6", "focaine5", "cteissier5@aboutads.info", "55", "Male", "Russia", "Quality Engineer", "147578.19", "3/25/2021", "true"},
	{"7", "vdehm6", "atatam6@youtube.com", "35", "Female", "United States", "Statistician II", "56544.67", "6/14/2019", "false"},
	{"8", "mlivzey7", "bhurford7@twitter.com", "46", "Female", "Kazakhstan", "Mechanical Systems Engineer", "99768.37", "1/28/2013", "false"},
	{"9", "etee8", "fmcmichael8@baidu.com", "20", "Male", "Portugal", "Paralegal", "112676.35", "10/26/2016", "true"},
	{"10", "emangin9", "blaydon9@go.com", "97", "Female", "Russia", "Structural Analysis Engineer", "52824.91", "4/10/2013", "false"},
	{"11", "acanedoa", "ncoccia@engadget.com", "37", "Male", "Honduras", "Account Representative IV", "108022.01", "12/21/2013", "true"},
	{"12", "vcoppb", "rbenardb@google.es", "51", "Female", "Colombia", "Technical Writer", "114171.5", "4/27/2013", "true"},
	{"13", "arenardc", "lhyslopc@trellian.com", "35", "Male", "China", "Account Executive", "116026.83", "8/26/2016", "false"},
	{"14", "tdowberd", "kwayted@live.com", "51", "Female", "China", "Geological Engineer", "141644.61", "3/12/2015", "false"},
	{"15", "lgreevese", "jliffee@timesonline.co.uk", "90", "Male", "Poland", "Systems Administrator II", "36717.32", "5/5/2012", "false"},
	{"16", "kmeardonf", "coshieldsf@princeton.edu", "43", "Female", "Ecuador", "Technical Writer", "124591.58", "7/8/2012", "false"},
	{"17", "rdehoogeg", "lmillwardg@comcast.net", "94", "Female", "France", "Librarian", "115100.59", "4/17/2014", "true"},
	{"18", "jtwohigh", "trubyh@uol.com.br", "28", "Male", "Portugal", "Professor", "97196.21", "6/5/2015", "true"},
	{"19", "etyti", "obagsteri@fda.gov", "42", "Genderfluid", "Bosnia and Herzegovina", "Software Consultant", "67911.3", "12/10/2021", "true"},
	{"20", "bhamflettj", "esilverlockj@posterous.com", "65", "Female", "Indonesia", "Food Chemist", "145683.88", "7/11/2010", "false"},
	{"21", "trandalesk", "pzaninik@time.com", "77", "Female", "China", "Social Worker", "84122.3", "2/1/2020", "true"},
	{"22", "bmcauslenel", "mbulwardl@clickbank.net", "52", "Male", "Peru", "VP Quality Control", "33026.4", "10/4/2015", "true"},
	{"23", "pspinckem", "jbarrattm@spotify.com", "67", "Male", "Sweden", "Senior Financial Analyst", "138209.93", "2/7/2019", "false"},
	{"24", "ncambridgen", "fbeazeyn@wikipedia.org", "26", "Genderqueer", "China", "Safety Technician III", "25949.78", "7/5/2018", "false"},
	{"25", "gcreigano", "arusselo@washington.edu", "92", "Female", "Argentina", "Executive Secretary", "105223.09", "5/8/2017", "false"},
	{"26", "jwalteringp", "cpolop@artisteer.com", "57", "Female", "Bolivia", "Speech Pathologist", "109258.76", "2/3/2021", "false"},
	{"27", "jpenvarneq", "cgerantq@icio.us", "37", "Bigender", "Russia", "Actuary", "98951.67", "7/17/2012", "false"},
	{"28", "whefferr", "clittleoverr@census.gov", "42", "Male", "Portugal", "Software Consultant", "115090.86", "2/12/2019", "true"},
	{"29", "wdonelds", "rdumsdays@hatena.ne.jp", "79", "Female", "Philippines", "Health Coach III", "65217.52", "1/14/2013", "false"},
	{"30", "mplowst", "ljarviet@ucsd.edu", "85", "Female", "China", "Associate Professor", "117399.94", "2/28/2013", "false"},
	{"31", "odurwardu", "hferou@networkadvertising.org", "91", "Female", "Czech Republic", "Senior Quality Engineer", "79771.12", "12/21/2014", "false"},
	{"32", "pharewoodv", "nvogelv@craigslist.org", "25", "Female", "Micronesia", "Product Engineer", "100584.61", "6/18/2015", "false"},
	{"33", "nblandamorew", "wsaltsbergw@mozilla.com", "31", "Male", "Tanzania", "Junior Executive", "54747.32", "4/8/2013", "true"},
	{"34", "wreanyx", "sebbersx@answers.com", "46", "Genderfluid", "Madagascar", "Geologist II", "147268.15", "12/8/2012", "true"},
	{"35", "ekoby", "jschurickey@youku.com", "30", "Female", "Sweden", "Research Assistant III", "126007.48", "10/10/2020", "true"},
	{"36", "sstainingz", "gwesleyz@techcrunch.com", "44", "Female", "Poland", "Senior Editor", "53201.25", "3/14/2022", "true"},
	{"37", "malanbrooke10", "tklulisek10@rambler.ru", "86", "Male", "Indonesia", "Sales Associate", "79172.83", "5/1/2022", "true"},
	{"38", "tfrancklyn11", "cstandbrooke11@mediafire.com", "84", "Female", "Poland", "Analog Circuit Design manager", "47422.75", "4/18/2019", "true"},
	{"39", "dabbatucci12", "lfulton12@discuz.net", "86", "Male", "Russia", "Tax Accountant", "56230.69", "7/9/2013", "true"},
	{"40", "hstubley13", "fdemkowicz13@g.co", "72", "Female", "United States", "Recruiting Manager", "111529.4", "7/20/2018", "false"},
	{"41", "lshilburne14", "smattes14@latimes.com", "43", "Male", "United States", "Mechanical Systems Engineer", "21464.75", "6/17/2012", "true"},
	{"42", "khodcroft15", "lcollie15@uiuc.edu", "90", "Female", "China", "Health Coach IV", "144377.87", "12/9/2012", "true"},
	{"43", "gborton16", "aenrique16@amazon.co.jp", "39", "Male", "Germany", "Safety Technician III", "23027.92", "12/11/2022", "true"},
	{"44", "jalbin17", "hlucio17@prlog.org", "55", "Female", "Russia", "Geological Engineer", "144131.41", "2/2/2021", "false"},
	{"45", "xashmore18", "ckavanagh18@creativecommons.org", "50", "Male", "Brazil", "Assistant Media Planner", "105664.24", "9/4/2014", "true"},
	{"46", "evooght19", "lbecken19@samsung.com", "67", "Female", "Costa Rica", "Senior Financial Analyst", "50445.73", "8/20/2014", "true"},
	{"47", "ispraging1a", "mtravers1a@google.co.jp", "38", "Male", "Colombia", "Mechanical Systems Engineer", "88924.95", "10/21/2017", "false"},
	{"48", "sweller1b", "fellens1b@qq.com", "67", "Male", "Ukraine", "Marketing Manager", "54330.36", "11/22/2011", "false"},
	{"49", "eklejin1c", "hkleanthous1c@linkedin.com", "46", "Female", "Panama", "Editor", "82786.17", "11/18/2017", "false"},
	{"50", "dderoberto1d", "hduffell1d@github.com", "42", "Female", "Mexico", "Environmental Specialist", "20995.21", "9/10/2014", "false"},
	{"51", "fdensham1e", "ktremayne1e@ftc.gov", "64", "Genderfluid", "Greece", "Research Associate", "61127.89", "10/17/2020", "true"},
	{"52", "rcoathup1f", "gdemattia1f@mail.ru", "86", "Female", "Nigeria", "Information Systems Manager", "84104.07", "5/24/2010", "true"},
	{"53", "hgrimestone1g", "swarman1g@umich.edu", "99", "Male", "Indonesia", "Media Manager IV", "22678.93", "1/27/2014", "false"},
	{"54", "nyetton1h", "kpedican1h@scribd.com", "87", "Female", "China", "VP Accounting", "123928.87", "6/16/2011", "true"},
	{"55", "eishaki1i", "tcheesworth1i@reddit.com", "40", "Female", "Greece", "Cost Accountant", "30380.23", "1/22/2022", "false"},
	{"56", "ttrewhitt1j", "rblanque1j@opera.com", "56", "Male", "Philippines", "Clinical Specialist", "139472.79", "11/14/2021", "true"},
	{"57", "kevason1k", "lmaccallester1k@biglobe.ne.jp", "31", "Male", "Niger", "Structural Analysis Engineer", "61518.37", "9/8/2018", "false"},
	{"58", "fnorsworthy1l", "gmaddie1l@tmall.com", "82", "Male", "Japan", "Help Desk Technician", "30574.89", "10/28/2015", "false"},
	{"59", "mgiscken1m", "cconcklin1m@google.nl", "93", "Female", "Mexico", "Cost Accountant", "56574.39", "5/13/2021", "true"},
	{"60", "hdisman1n", "chorley1n@hp.com", "76", "Male", "Indonesia", "Engineer I", "104352.1", "6/14/2021", "false"},
	{"61", "dstelfax1o", "lkenford1o@360.cn", "97", "Polygender", "China", "Associate Professor", "94708.59", "6/1/2016", "true"},
	{"62", "bseares1p", "rsherburn1p@gmpg.org", "57", "Male", "Colombia", "Software Test Engineer II", "20267.37", "1/14/2013", "false"},
	{"63", "cmorsey1q", "lcorten1q@netlog.com", "82", "Female", "Australia", "Staff Accountant I", "119745.92", "11/21/2022", "true"},
	{"64", "gverbeek1r", "ghamnett1r@livejournal.com", "32", "Male", "Vietnam", "Staff Accountant III", "115246.46", "9/3/2015", "true"},
	{"65", "hhenbury1s", "jshephard1s@google.pl", "98", "Female", "Russia", "Software Consultant", "71309.98", "7/1/2013", "true"},
	{"66", "wenoch1t", "arutherford1t@ocn.ne.jp", "45", "Male", "Sierra Leone", "Internal Auditor", "109874.98", "11/17/2011", "true"},
	{"67", "msymers1u", "dsemonin1u@bloglines.com", "91", "Female", "Philippines", "Research Associate", "52802.49", "11/1/2010", "true"},
	{"68", "rfurber1v", "kchipps1v@unesco.org", "48", "Male", "Syria", "Staff Scientist", "110117.96", "4/13/2022", "false"},
	{"69", "mponsford1w", "bfairall1w@hibu.com", "28", "Male", "Ukraine", "Senior Developer", "20595.36", "5/2/2016", "false"},
	{"70", "amerill1x", "efrudd1x@acquirethisname.com", "56", "Female", "Belarus", "Web Designer II", "122760.62", "2/24/2018", "true"},
	{"71", "jofarrell1y", "choneywood1y@furl.net", "100", "Male", "Peru", "Safety Technician I", "65262.68", "9/20/2016", "false"},
	{"72", "cgrane1z", "ytomasoni1z@qq.com", "72", "Male", "Portugal", "Speech Pathologist", "30136.18", "11/7/2021", "false"},
	{"73", "mlorens20", "ishearwood20@vkontakte.ru", "18", "Non-binary", "Indonesia", "Technical Writer", "115490.87", "10/6/2013", "false"},
	{"74", "wstellino21", "jchaise21@utexas.edu", "63", "Female", "Czech Republic", "Web Developer I", "30671.43", "5/28/2012", "true"},
	{"75", "lbesantie22", "fmackowle22@purevolume.com", "59", "Female", "Brazil", "Account Coordinator", "124831.76", "10/24/2011", "true"},
	{"76", "ahaverty23", "ebevis23@discovery.com", "65", "Male", "Costa Rica", "Senior Developer", "142537.75", "9/13/2011", "false"},
	{"77", "meat24", "rheart24@opensource.org", "70", "Female", "Philippines", "Marketing Manager", "92026.81", "12/28/2014", "true"},
	{"78", "mplover25", "emoxley25@samsung.com", "57", "Male", "Poland", "Food Chemist", "112850.57", "7/27/2019", "false"},
	{"79", "ljaye26", "rkilshall26@si.edu", "55", "Male", "Malawi", "Professor", "131157.57", "4/14/2013", "false"},
	{"80", "ccrackett27", "tdagworthy27@1und1.de", "42", "Male", "Egypt", "Desktop Support Technician", "24440.54", "4/20/2014", "true"},
	{"81", "ngrissett28", "rlogan28@usda.gov", "45", "Male", "Paraguay", "Accountant III", "141839.36", "5/12/2017", "true"},
	{"82", "rbaugh29", "cradwell29@ihg.com", "91", "Male", "Ukraine", "Structural Engineer", "35321.43", "8/25/2019", "true"},
	{"83", "lgawkroge2a", "mbortolotti2a@reddit.com", "38", "Female", "China", "Health Coach II", "65292.11", "6/29/2011", "false"},
	{"84", "osandeford2b", "jmelloi2b@elpais.com", "79", "Female", "Mexico", "Executive Secretary", "116933.12", "8/15/2017", "true"},
	{"85", "gcleobury2c", "ccaulkett2c@1und1.de", "90", "Female", "Philippines", "GIS Technical Architect", "86581.17", "9/7/2010", "true"},
	{"86", "rmieville2d", "ghindhaugh2d@reuters.com", "99", "Male", "Cuba", "Compensation Analyst", "65695.87", "6/10/2011", "false"},
	{"87", "plapere2e", "shairsine2e@thetimes.co.uk", "56", "Male", "Thailand", "Software Engineer III", "24620.02", "3/11/2021", "false"},
	{"88", "vsparks2f", "favarne2f@amazon.co.jp", "31", "Male", "Russia", "Registered Nurse", "64694.77", "4/25/2012", "true"},
	{"89", "gdulling2g", "schippendale2g@netlog.com", "20", "Male", "Brazil", "Junior Executive", "29792.19", "4/14/2012", "true"},
	{"90", "cmaliffe2h", "jsacco2h@mayoclinic.com", "94", "Male", "Philippines", "VP Marketing", "58018.14", "4/10/2011", "false"},
	{"91", "aosorio2i", "mfarnhill2i@yandex.ru", "71", "Male", "Morocco", "Analog Circuit Design manager", "104667.46", "6/28/2013", "false"},
	{"92", "ttouzey2j", "wgentner2j@addtoany.com", "43", "Male", "Philippines", "Account Coordinator", "43486.91", "4/4/2022", "false"},
	{"93", "kmusla2k", "ltapson2k@a8.net", "30", "Female", "Mexico", "Nurse", "92451.65", "3/19/2016", "true"},
	{"94", "briggeard2l", "amcgarrahan2l@merriam-webster.com", "67", "Female", "Iran", "Operator", "20632.6", "12/17/2011", "true"},
	{"95", "ilandrean2m", "anoyce2m@clickbank.net", "21", "Polygender", "France", "Senior Cost Accountant", "134853.7", "6/14/2012", "true"},
	{"96", "jlewton2n", "gbamfield2n@live.com", "37", "Female", "Indonesia", "Information Systems Manager", "105050.66", "7/28/2014", "false"},
	{"97", "ahawksworth2o", "vgittings2o@google.com", "67", "Male", "Georgia", "Financial Advisor", "99626.93", "9/27/2012", "true"},
	{"98", "miacovozzo2p", "ypretsel2p@illinois.edu", "37", "Bigender", "Russia", "Operator", "61405.17", "4/16/2013", "false"},
	{"99", "vrickardes2q", "csparshutt2q@ning.com", "83", "Male", "Indonesia", "Office Assistant IV", "53883.45", "6/17/2022", "true"},
	{"100", "ismalecombe2r", "bquilty2r@list-manage.com", "19", "Male", "China", "Community Outreach Specialist", "63102.84", "9/7/2020", "true"},
}

func CopyColumn(col *types.DataColumn) *types.DataColumn {

	cellVal := &types.CellValue{
		DataType: col.CellValue.DataType,
	}
	switch cellVal.DataType {
	case types.IntType:
		cellVal.IntValue = col.CellValue.IntValue
		break
	case types.LongType:
		cellVal.LongValue = col.CellValue.LongValue
		break
	case types.TimestampType:
		cellVal.TimestampValue = col.CellValue.TimestampValue
		break
	case types.StringType:
		cellVal.StringValue = col.CellValue.StringValue
		break
	case types.DoubleType:
		cellVal.DoubleValue = col.CellValue.DoubleValue
		break
	case types.BoolType:
		cellVal.BoolValue = col.CellValue.BoolValue
		break
	}

	newCol := &types.DataColumn{
		ColumnName: col.ColumnName,
		CellValue:  cellVal,
	}

	return newCol
}

func PrintDataset(ds *types.DataSet) {
	if len(ds.Rows) < 1 {
		fmt.Println("=============== NO DATA ===============")
		return
	}

	// print headers here
	row0 := ds.Rows[0]
	for _, col := range row0.Columns {
		fmt.Print(col.ColumnName)
		fmt.Print("  |  ")
	}
	fmt.Println("")
	fmt.Println("----------------------------------------")
	for _, r := range ds.Rows {
		for _, c := range r.Columns {
			fmt.Print(CellDataToString(c.CellValue))
			fmt.Print("  |  ")
		}
		fmt.Println("")
		fmt.Println("----------------------------------------")
	}
}

func GetColumn(r *types.DataRow, col string) *types.DataColumn {
	for _, c := range r.Columns {
		if c.ColumnName == col {
			return c
		}
	}

	return nil
}

func IsEqualToInterfaceVal(cell *types.CellValue, val interface{}) bool {
	if cell == nil && val == nil {
		return true
	}

	if cell == nil {
		return false
	}

	switch cell.DataType {
	case types.IntType:
		if w, ok := val.(int32); ok {
			return w == cell.IntValue
		}
		return false
	case types.LongType:
		if w, ok := val.(int64); ok {
			return w == cell.LongValue
		}
		return false
	case types.TimestampType:
		if w, ok := val.(time.Time); ok {
			return cell.TimestampValue.Equal(w)
		}
		return false
	case types.StringType:
		if w, ok := val.(string); ok {
			return w == cell.StringValue
		}
		return false
	case types.DoubleType:
		if w, ok := val.(float64); ok {
			return w == cell.DoubleValue
		}
		return false
	case types.BoolType:
		if w, ok := val.(bool); ok {
			return w == cell.BoolValue
		}
		return false
	case types.NilType:
		return val == nil

	}

	return false
}

func CellDataToString(cell *types.CellValue) string {
	if cell == nil {
		return ""
	}

	switch cell.DataType {
	case types.IntType:
		return strconv.FormatInt(int64(cell.IntValue), 10)
	case types.LongType:
		return strconv.FormatInt(cell.LongValue, 10)
	case types.TimestampType:
		return cell.TimestampValue.String()
	case types.StringType:
		return cell.StringValue
	case types.DoubleType:
		return strconv.FormatFloat(cell.DoubleValue, 'f', 6, 64)
	case types.BoolType:
		if cell.BoolValue {
			return "true"
		}
		return "false"
	case types.NilType:
		return ""

	}

	return ""
}

func loadCSVFile(filePath string, splitHeaders bool) (headers []string, dataset [][]string, err error) {
	var ior io.Reader
	ior, err = os.Open(filePath)
	if err != nil {
		return nil, nil, err
	}
	// let's read columns
	rc, ok := ior.(io.ReadCloser)
	defer rc.Close()
	if !ok {
		return nil, nil, errors.New("file error possibly huh?")
	}

	buf := bufio.NewReader(ior)
	csvr := csv.NewReader(buf)
	csvr.TrailingComma = true
	if splitHeaders {
		headers, err = csvr.Read()
		if err != nil {
			return nil, nil, err
		}
	}
	// now lets load the complete file into memory
	dataset = make([][]string, 0, 10000)
	for {
		var row []string
		row, err = csvr.Read()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return headers, dataset, err
		}
		dataset = append(dataset, row)
	}

}

func LoadCSVFileFromTestDataDir(fullPath string, splitHeaders bool) (headers []string, dataset [][]string, err error) {
	return loadCSVFile(fullPath, splitHeaders)
}

func DownloadFile(url string, target string) error {
	out, err := os.Create(target)
	if err != nil {
		return err
	}
	defer out.Close()
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}
	return nil
}

func DownloadZipFileIfNotExists(url string, zipPath string, filePath string) error {
	_, err := os.Stat(filePath)
	if err == nil {
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	_, err = os.Stat(zipPath)
	if err == nil {
		return Unzip(zipPath, filePath)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	err = DownloadFile(url, zipPath)
	if err != nil {
		return err
	}
	return Unzip(zipPath, filePath)
}

func Unzip(src, dest string) error {
	r, err := zip.OpenReader(src)
	if err != nil {
		return err
	}
	defer func() {
		if err := r.Close(); err != nil {
			panic(err)
		}
	}()

	// Closure to address file descriptors issue with all the deferred .Close() methods
	extractAndWriteFile := func(f *zip.File) error {
		rc, err := f.Open()
		if err != nil {
			return err
		}
		defer func() {
			if err := rc.Close(); err != nil {
				panic(err)
			}
		}()

		path := filepath.Join(dest)

		if f.FileInfo().IsDir() {
		} else {
			f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
			if err != nil {
				return err
			}
			defer func() {
				if err := f.Close(); err != nil {
					panic(err)
				}
			}()

			_, err = io.Copy(f, rc)
			if err != nil {
				return err
			}
		}
		return nil
	}

	err = extractAndWriteFile(r.File[0])
	if err != nil {
		return err
	}

	return nil
}
