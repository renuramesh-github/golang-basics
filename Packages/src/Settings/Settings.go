/**
 * -------------------------------------------------------------------------------
 * @author Dileep
 * Copyright 2018 The Epixelsolutions.pvt.ltd. All rights reserved.
 *
 * Settings for the project
 * -------------------------------------------------------------------------------
 */
package Settings

// DB connection structure
// Store DB settings
type DBConnection struct {
	Host, Port, Database_name, Username, Password string
}

// Store Mongo DB connection
type MongoDBConnection struct {
	Host, Port, Database_name, Username, Password string
}

type DBHisConnection struct {
	Host, Port, Database_name, Username, Password string
}

// Store the site settings
type SiteData struct {
	siteName, SiteBaseURL, SitePublished, GoRunPath string
}

var (

	/**
	 * Database configuration comes here
	 * Host : Host for the database
	 * Port : TCP port
	 * Database_name : Database name
	 * Username : username
	 * Password : database password
	 */
	//for staging
	// DB = DBConnection{
	// 	Host:          "localhost",
	// 	Port:          "2222",
	// 	Database_name: "epixel_club_monoline_plus",
	// 	Username:      "epixel_club_mono",
	// 	Password:      "d+ro60=l#tAP",
	// }
	//for Load Testing Purpose
	// DB = DBConnection{
	// 	Host:          "localhost",
	// 	Port:          "2222",
	// 	Database_name: "epixel_club_monoline_plus",
	// 	Username:      "clubgo",
	// 	Password:      "club0guy26TT545ew",
	// }
	/*---------- Local Settings ------------*/
	//Data base
	// DB = DBConnection{
	// 	Host: "10.0.0.6",
	// 	Port: "2222",
	// 	// Database_name: "club_line_up_monoline",
	// 	Database_name: "ClubLIVEeeeSADDBE",
	// 	Username:      "root",
	// 	Password:      "root",
	// }

	//For script run
	// DB = DBConnection{
	// 	Host:          "localhost",
	// 	Port:          "2222",
	// 	Database_name: "club_live_real",
	// 	Username:      "root",
	// 	Password:      "root",
	// }

	DB = DBConnection{
		Host: "localhost",
		Port: "2222",
		// Database_name: "ClubLIVEeeeSADDBE_latest",
		Database_name: "ClubLIVEeeeSADDBE_new",
		// Database_name: "ClubLIVEeeeSADDBE_2021-4-7",
		Username: "root",
		Password: "root",
	}
	DBH = DBHisConnection{
		Host:          "localhost",
		Port:          "2222",
		Database_name: "club_live_real_test",
		Username:      "root",
		Password:      "root",
	}
	//mongo db
	Mongodb = MongoDBConnection{
		Host:          "localhost",
		Port:          "27017",
		Database_name: "clubmono",
		Username:      "clubadmin",
		Password:      "Epix8lAsclubAd1m1n",
	}
	/*----------  Staging Settings ------------*/

	// DB = DBConnection{
	// 	Host:          "localhost",
	// 	Port:          "2222",
	// 	Database_name: "epixel_club_monoline_plus",
	// 	// Username:      "for_go",
	// 	Username: "epixel_club_mono",
	// 	// Password: "uhfWRkdh4377hADJfd4f",
	// 	Password: "d+ro60=l#tAP",
	// }

	// Mongodb = MongoDBConnection{
	// 	Host:          "localhost",
	// 	Port:          "27017",
	// 	Database_name: "clubmono",
	// 	Username:      "clubadmin",
	// 	Password:      "Epix8lAsclubAd1m1n",
	// }
	/*---------- Staging Settings (AWS) ------------*/

	// DB = DBConnection{
	// 	// Host:          "club-staging-rds.c3g4gwnojavx.us-east-2.rds.amazonaws.com",
	// 	Host:          "staging-club-rds.c3g4gwnojavx.us-east-2.rds.amazonaws.com",
	// 	Port:          "3306",
	// 	Database_name: "club_monolinedb",
	// 	Username:      "clubadmin",
	// 	Password:      "G7Yna2plByuItG9CvqM",
	// }
	// DBH = DBHisConnection{
	// 	Host:          "staging-club-rds.c3g4gwnojavx.us-east-2.rds.amazonaws.com",
	// 	Port:          "3306",
	// 	Database_name: "clubstage_monolinedbcompare",
	// 	Username:      "clubadmin",
	// 	Password:      "G7Yna2plByuItG9CvqM",
	// }

	// Mongodb = MongoDBConnection{
	// 	Host:          "localhost",
	// 	Port:          "27017",
	// 	Database_name: "clubstagingDB",
	// 	Username:      "stagingclubadmin",
	// 	Password:      "XcYupix8lAscsd9IjRgN2f",
	// }

	/*---------- Live Settings ------------*/
	// DB = DBConnection{
	// 	Host:          "localhost",
	// 	Port:          "2222",
	// 	Database_name: "ClubLIVEeeeSADDBE",
	// 	Username:      "EPSCLUBGOgoEngine",
	// 	Password:      "fgdt1dfwedfdsf4esdfssd",
	// }

	// Mongodb = MongoDBConnection{
	// 	Host:          "localhost",
	// 	Port:          "27017",
	// 	Database_name: "clubLiveDBs",
	// 	Username:      "LIVEclubadmin",
	// 	Password:      "WEsdfpix8lAscsdfslubAfe",
	// }

	// /*---------- Live Settings (AWS) ------------*/

	// DB = DBConnection{
	// 	Host:          "clubmono-planet.czppp8swwswi.us-east-1.rds.amazonaws.com",
	// 	Port:          "3306",
	// 	Database_name: "clubmegaplanet_DB",
	// 	Username:      "clubgouser",
	// 	Password:      "1Erj1dfwedfdsf4esdtcpkd",
	// }
	// DBH = DBHisConnection{
	// 	Host:          "clubmono-planet.czppp8swwswi.us-east-1.rds.amazonaws.com",
	// 	Port:          "3306",
	// 	Database_name: "clubmegaplanet_history_DB",
	// 	Username:      "clubgouser",
	// 	Password:      "1Erj1dfwedfdsf4esdtcpkd",
	// }

	// Mongodb = MongoDBConnection{
	// 	Host:          "34.0.46.4",
	// 	Port:          "27017",
	// 	Database_name: "clubLiveDBs",
	// 	Username:      "LIVEclubadmin",
	// 	Password:      "WEsdfpix8lAscsdfslubAfe",
	// }

	/**
	 * Site configurations comes here
	 * siteName : Site name
	 * SiteBaseURL : Site base url
	 * SitePublished : Site published time
	 */
	Site = SiteData{
		siteName:      "Club Line Up",
		SiteBaseURL:   "2222",
		SitePublished: "2006-01-02 15:04:05",
		GoRunPath:     "./cron-go",
	}
)
