package phpserialize

import (
	"bytes"
	"errors"
	"reflect"
	"strconv"
	"strings"
	
)

// findByte will return the first position at or after offset of the specified
// byte. -1 is returned if the byte is not found.
func findByte(data []byte, lookingFor byte, offset int) int {
	for ; offset < len(data); offset++ {
		if data[offset] == lookingFor {
			return offset
		}
	}

	return -1
}

// DecodePHPString converts a string of ASCII bytes (like "Bj\xc3\xb6rk") back
// into a UTF8 string ("Björk", in that case).
func DecodePHPString(data []byte) string {
	var buffer bytes.Buffer
	for i := 0; i < len(data); i++ {
		if data[i] == '\\' {
			b, _ := strconv.ParseInt(string(data[i+2:i+4]), 16, 32)
			buffer.WriteByte(byte(b))
			i += 3
		} else {
			buffer.WriteByte(data[i])
		}
	}

	return buffer.String()
}

func UnmarshalFloat(data []byte) (float64, error) {
	i, _, err := consumeFloat(data, 0)
	return i, err
}

func UnmarshalString(data []byte) (string, error) {
	i, _, err := consumeString(data, 0)
	return i, err
}

func UnmarshalBytes(data []byte) ([]byte, error) {
	v, err := UnmarshalString(data)

	return []byte(v), err
}

func UnmarshalInt(data []byte) (int64, error) {
	i, _, err := consumeInt(data, 0)
	return i, err
}

func UnmarshalUint(data []byte) (uint64, error) {
	v, err := UnmarshalInt(data)
	return uint64(v), err
}

func UnmarshalNil(data []byte) error {
	_, _, err := consumeNil(data, 0)
	return err
}

func UnmarshalBool(data []byte) (bool, error) {
	v, _, err := consumeBool(data, 0)
	return v, err
}

func checkType(data []byte, typeCharacter byte, offset int) bool {
	return len(data) > offset && data[offset] == typeCharacter
}

func UnmarshalArray(data []byte) ([]interface{}, error) {
	v, _, err := consumeArray(data, 0)

	return v, err
}

func UnmarshalAssociativeArray(data []byte) (map[interface{}]interface{}, error) {
	// We may be unmarshalling an object into a map.
	if checkType(data, 'O', 0) {
		result, _, err := consumeObjectAsMap(data, 0)

		return result, err
	}

	if !checkType(data, 'a', 0) {
		return map[interface{}]interface{}{},
			errors.New("not an array or object")
	}

	rawLength, offset := consumeStringUntilByte(data, ':', 2)
	length, err := strconv.Atoi(rawLength)
	if err != nil {
		return map[interface{}]interface{}{}, err
	}

	// Skip over the ":{"
	offset += 2

	result := map[interface{}]interface{}{}
	for i := 0; i < length; i++ {
		var key interface{}

		key, offset, err = consumeNext(data, offset)
		if err != nil {
			return map[interface{}]interface{}{}, err
		}

		result[key], offset, err = consumeNext(data, offset)
		if err != nil {
			return map[interface{}]interface{}{}, err
		}
	}

	return result, nil
}

func UnmarshalObject(data []byte, v interface{}) error {
	_, err := consumeObject(data, 0, v)
	return err
}

func Unmarshal(data []byte, v interface{}) error {
	value := reflect.ValueOf(v).Elem()
  
  
		v, err := UnmarshalInt(data)
    
		if err != nil {
			v1, err := UnmarshalUint(data)
			if err != nil {

				v2, err := UnmarshalBool(data)
				if err != nil {

					v3, err := UnmarshalString(data)
					
					if err != nil {
						v4, err := UnmarshalFloat(data)

						if err != nil {
							return err
						}else{
							float_str := strconv.FormatFloat(v4, 'f', 6, 64)
							value.SetString(float_str)
						}
					}else{
					  value.SetString(v3)
					}
				}else{
					 bool_str := strconv.FormatBool(v2)
					 value.SetString(bool_str)
				}
			}else{
				uintD_str := strconv.Itoa(int(v1))
				value.SetString(uintD_str)
			}
		}else{
				i_uint := v
				i_uint_str := strconv.Itoa(int(i_uint.(int64)))
				value.SetString(i_uint_str)
		}
    
		//value.SetInt(v)
	/*	i_int := int(v)
		i_str := strconv.Itoa(i_int)
		value.SetString(i_str)*/


	/*case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v, err := UnmarshalUint(data)
		if err != nil {
			return err
		}

		//value.SetUint(v)

		i_uint := int(v)
		i_uint_str := strconv.Itoa(i_uint)
		value.SetString(i_uint_str)


	case reflect.Float32, reflect.Float64:
		v, err := UnmarshalFloat(data)
		if err != nil {
			return err
		}

		//value.SetFloat(v)

		float_str := strconv.FormatFloat(v, 'f', 6, 64)
		value.SetString(float_str)

	case reflect.Bool:
		v, err := UnmarshalBool(data)
		if err != nil {
			return err
		}
    
    float_str := strconv.FormatBool(v)
		value.SetString(float_str)

		//value.SetBool(v)

	case reflect.String:
		v, err := UnmarshalString(data)
		if err != nil {
			return err
		}

		value.SetString(v)

	case reflect.Slice:
		// uint8 is an alias for byte. This means we are trying to pull
		// a binary string out.
		if value.Type().Elem().Kind() == reflect.Uint8 {
			v, err := UnmarshalBytes(data)
			if err != nil {
				return err
			}

			value.SetBytes(v)
			return nil
		}

		// Otherwise this must be a slice (array)
		v, err := UnmarshalArray(data)
		if err != nil {
			return err
		}

		value.Set(reflect.ValueOf(v))
		return nil

	case reflect.Map:
		v, err := UnmarshalAssociativeArray(data)
		if err != nil {
			return err
		}

		value.Set(reflect.ValueOf(v))
		return nil

	case reflect.Struct:
		err := UnmarshalObject(data, v)
		if err != nil {
			return err
		}

		return nil

	default:
		return errors.New("can not unmarshal type: " + value.Kind().String())
	}*/

	return nil
}

func upperCaseFirstLetter(s string) string {
	return strings.ToUpper(s[0:1]) + s[1:]
}
