package mongodb

import (
	"Settings"
	// U "Utility"
	"context"
	"errors"
	"fmt"
	"log"
	"os/exec"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func SetMongohost(host string) (status bool, Err error) {
	//trim the string
	host = strings.TrimSpace(host)
	//check the hostname is empty or not
	if len(host) <= 0 {
		return false, errors.New("error: Host name seems to be empty")
	}

	//check the hostname exist or not using ping
	out, _ := exec.Command("ping", host, "-c 5", "-i 3", "-w 10").Output()
	if strings.Contains(string(out), "Destination Host Unreachable") {
		return false, errors.New("error: Destination Host Unreachable")
	}

	//if all ok then set the hostname
	if host != "" {
		Settings.Mongodb.Host = host
	}
	//No error
	return true, nil
}

func Getmongohost() string {
	return Settings.Mongodb.Host
}

func SetMongoport(port string) (status bool, Err error) {
	//trim the string
	port = strings.TrimSpace(port)
	//check the hostname is empty or not
	if len(port) <= 0 {
		return false, errors.New("error: port name seems to be empty")
	}

	//if all ok then set the hostname
	if port != "" {
		Settings.Mongodb.Port = port
	}
	//No error
	return true, nil
}

func Getmongoport() string {
	return Settings.Mongodb.Port
}

func Setuser(user string) (status bool, Err error) {
	//trim the string
	user = strings.TrimSpace(user)
	//check the hostname is empty or not
	if len(user) <= 0 {
		return false, errors.New("error: user name seems to be empty")
	}

	//if all ok then set the hostname
	if user != "" {
		Settings.Mongodb.Username = user
	}
	//No error
	return true, nil
}

func Getuser() string {
	return Settings.Mongodb.Username
}

func SetMongodb(db string) (status bool, Err error) {
	//trim the string
	db = strings.TrimSpace(db)
	//check the hostname is empty or not
	if len(db) <= 0 {
		return false, errors.New("error: db name seems to be empty")
	}

	//if all ok then set the hostname
	if db != "" {
		Settings.Mongodb.Port = db
	}
	//No error
	return true, nil
}

func Getdb() string {
	return Settings.Mongodb.Database_name
}

func Setpass(pw string) (status bool, Err error) {
	//trim the string
	pw = strings.TrimSpace(pw)
	//check the hostname is empty or not
	if len(pw) <= 0 {
		return false, errors.New("error: password seems to be empty")
	}

	//if all ok then set the hostname
	if pw != "" {
		Settings.Mongodb.Password = pw
	}
	//No error
	return true, nil
}

func Getpass() string {
	return Settings.Mongodb.Password
}

func Createconnection() (*mongo.Client, error) {

	// ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	// defer cancel()

	Host := Getmongohost()
	// Port := Getmongoport()
	Dbname := Getdb()
	Username := Getuser()
	Password := Getpass()
	uri := fmt.Sprintf("mongodb://%v:%v@%v/?authSource=%v", Username, Password, Host, Dbname)

	//client, err := mongo.NewClient(options.Client().ApplyURI(uri))

	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(uri))

	// defer client.Disconnect(ctx)

	db := client.Database(Dbname)

	fmt.Println(db)

	if err != nil {
		log.Fatal(err)
	}

	// Check the connection
	err = client.Ping(context.TODO(), nil)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Connected to MongoDB!")
	return client, err
}

func Getcollecton(name string) *mongo.Collection {

	Dbname := Getdb()
	getconnection, err := Createconnection()

	if err != nil {
		log.Fatal(err)
	}

	collection := getconnection.Database(Dbname).Collection(name)
	return collection

}

func GetcollectonObj(name string, conn *mongo.Client) *mongo.Collection {

	Dbname := Getdb()
	collection := conn.Database(Dbname).Collection(name)
	return collection

}

func Insertone(collectionname string, document bson.D) error {
	collection := Getcollecton(collectionname)

	_, err := collection.InsertOne(context.Background(), document)
	// _, err := colle.InsertOne(context.TODO(), document)
	if err != nil {
		log.Fatal(err)
	}

	return err
}
func InsertoneObj(collectionObj *mongo.Collection, document bson.D) error {

	_, err := collectionObj.InsertOne(context.Background(), document)
	// _, err := colle.InsertOne(context.TODO(), document)
	if err != nil {
		log.Fatal(err)
	}

	return err
}
func Insertmany(collectionname string, document []interface{}) error {

	collection := Getcollecton(collectionname)
	_, err := collection.InsertMany(context.Background(), document)

	return err
}

func Findall(collectionname string, document bson.D) (*mongo.Cursor, error) {

	// ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	// defer cancel()
	collection := Getcollecton(collectionname)

	data, err := collection.Find(context.Background(), document)
	// data.Next(context.Background())
	// if data.Next(ctx) {
	// 	// fmt.Println("--- raw bson from db: ", data.Current)
	// }

	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("sadasdsadasdsad", data.Current)
	// fmt.Println("data", data)

	return data, err
}

func Findone(collectionname string, filter interface{}, result bson.D) error {

	collection := Getcollecton(collectionname)

	err := collection.FindOne(context.TODO(), filter).Decode(&result)

	if err != nil {
		log.Fatal(err)
	}
	return err
}

func Findoneandreplace(collectionname string, filter interface{}, replacement interface{}, result bson.D) error {

	collection := Getcollecton(collectionname)
	err := collection.FindOneAndReplace(context.Background(), filter, replacement).Decode(&result)

	if err != nil {
		log.Fatal(err)
	}
	return err
}

func Findoneandupdate(collectionname string, filter interface{}, update interface{}, result bson.D) error {

	collection := Getcollecton(collectionname)

	err := collection.FindOneAndUpdate(context.Background(), filter, update).Decode(&result)

	if err != nil {
		log.Fatal(err)
	}
	return err

}

func Findoneanddelete(collectionname string, filter interface{}, result bson.D) error {

	collection := Getcollecton(collectionname)

	err := collection.FindOneAndDelete(context.Background(), filter).Decode(&result)

	if err != nil {
		log.Fatal(err)
	}
	return err
}

// func Findandmodify(collectionname string, update bson.M) {
// 	change := mongo.Change{
// 		Update:    bson.M{"$inc": bson.M{"n": 1}},
// 		ReturnNew: false,
// 	}
// 	info, err := col.Find(M{"_id": id}).Apply(change, &doc)
// 	fmt.Println(doc.N)
// }

func Updateone(collectionname string, filter interface{}, newdata interface{}) (*mongo.UpdateResult, error) {

	collection := Getcollecton(collectionname)

	result, err := collection.UpdateOne(context.Background(), filter, newdata)

	if err != nil {
		log.Fatal(err)
	}

	return result, err
}

func Updatemany(collectionname string, filter interface{}, newdata interface{}) (*mongo.UpdateResult, error) {

	collection := Getcollecton(collectionname)

	result, err := collection.UpdateMany(context.Background(), filter, newdata)
	if err != nil {
		log.Fatal(err)
	}

	return result, err

}

func Replaceone(collectionname string, filter interface{}, replacement interface{}) (*mongo.UpdateResult, error) {

	collection := Getcollecton(collectionname)

	result, err := collection.ReplaceOne(context.Background(), filter, replacement)
	if err != nil {
		log.Fatal(err)
	}

	return result, err
}

func DeleteOne(collectionname string, filter interface{}) (*mongo.DeleteResult, error) {
	collection := Getcollecton(collectionname)
	deleteResult, err := collection.DeleteOne(context.TODO(), filter)
	if err != nil {
		log.Fatal(err)
	}
	return deleteResult, err
}

func Deletemany(collectionname string, filter interface{}) (*mongo.DeleteResult, error) {

	collection := Getcollecton(collectionname)

	deleteResult, err := collection.DeleteMany(context.TODO(), filter)

	if err != nil {
		log.Fatal(err)
	}
	return deleteResult, err
}

// func Dropcollection(collectionname string) error {

// 	collection := Getcollecton(collectionname)
// 	err := collection.Drop(context.Background())
// 	fmt.Println(err)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	return err
// }
func CloseConnection(client *mongo.Client) error {

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := client.Disconnect(ctx)
	fmt.Println(err)

	if err != nil {
		log.Fatal(err)
	}
	return err
}
