package main

import (
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
)

func roundFloat(val float64, precision int) float64 {
	ratio := math.Pow(10, float64(precision))
	return math.Round(val*ratio) / ratio
}

func ValidarNumero(valor string, campo string) (bool, error) {
	_, err := strconv.ParseFloat(valor, 64)
	if err != nil {
		return false, fmt.Errorf("el valor %s del campo %s no es un numero", valor, campo)
	}
	return true, nil
}

func ValoresPosibles(valor string, valoresPosibles []string, campo string) (bool, error) {
	for _, valorPosible := range valoresPosibles {
		if valor == valorPosible {
			return true, nil
		}
	}
	return false, fmt.Errorf("el valor %s del campo %s no es un valor posible", valor, campo)
}

func LongitudMaxima(valor string, long string, campo string) (bool, error) {
	longitud, _ := strconv.Atoi(long)
	if len(valor) > longitud {
		return false, fmt.Errorf("el valor %s del campo %s supera la longitud máxima de %d", valor, campo, longitud)
	}
	return true, nil
}

func ValidarCaracteresEspeciales(texto string, campo string) (bool, error) {
	for _, letra := range texto {
		if (letra < 'a' || letra > 'z') && (letra < 'A' || letra > 'Z') {
			return false, fmt.Errorf("el texto del campo %s contiene caracteres especiales", campo)
		}
	}
	return true, nil
}

func FormulaIgualdadNumero(valor string, decimales string, valorComparacion string, campo string) (bool, error) {

	num1, _ := strconv.ParseFloat(valor, 8)

	num2, _ := strconv.ParseFloat(valorComparacion, 8)

	dec, _ := strconv.Atoi(decimales)

	if roundFloat(num1, dec) == roundFloat(num2, dec) {
		return true, nil
	}

	return false, fmt.Errorf("el valor %s del campo %s no es igual al valor %s", valor, campo, valorComparacion)

}

func FormulaIgualdadTexto(valor string, valorComparacion string, campo string) (bool, error) {

	if valor == valorComparacion {
		return true, nil
	}

	return false, fmt.Errorf("el valor %s del campo %s no es igual al valor %s", valor, campo, valorComparacion)

}

func LongitudMinima(texto string, long string, campo string) (bool, error) {
	longitud, _ := strconv.Atoi(long)
	if len(texto) < longitud {
		return false, fmt.Errorf("el valor %s del campo %s no supera la longitud mínima de %d", texto, campo, longitud)
	}
	return true, nil
}

func ValidarDocumento(documento string, tipoDocumento string, campo string) (bool, error) {
	var longitud int
	tipo, _ := strconv.Atoi(tipoDocumento)

	switch tipo {
	case 2:
		longitud = 8
	default:
		longitud = 10
	}
	if len(documento) == longitud {
		return true, nil
	}

	return false, fmt.Errorf("el valor %s del campo %s no es un documento válido", documento, campo)
}

func ValidarFechaMaxima(fecha string, fechaComparacion string, formato string, campo string) (bool, error) {
	fechaCmp, err := time.Parse(formato, fechaComparacion)
	if err != nil {
		return false, fmt.Errorf("fecha de comparacion no cumple con el formato %s", formato)
	}

	fechaGo, err := time.Parse(formato, fecha)

	if err != nil {
		return false, fmt.Errorf("fecha del campo %s no cumple con el formato %s", campo, formato)
	}

	if fechaGo.After(fechaCmp) {
		return false, fmt.Errorf("fecha %s del campo %s es mayor a %s", fecha, campo, fechaComparacion)
	}

	return true, nil

}

func ValidarFechaMinima(fecha string, fechaComparacion string, formato string, campo string) (bool, error) {
	fechaCmp, err := time.Parse(formato, fechaComparacion)
	if err != nil {
		return false, err
	}

	fechaGo, err := time.Parse(formato, fecha)

	if err != nil {
		return false, err
	}

	if fechaGo.Before(fechaCmp) {
		return false, fmt.Errorf("fecha %s del campo %s es menor a %s", fecha, campo, fechaComparacion)
	}

	return true, nil

}

func ValidarFormatoFecha(fecha string, formato string, campo string) (bool, error) {
	_, err := time.Parse(formato, fecha)
	if err != nil {
		return false, fmt.Errorf("fecha %s del campo %s no cumple con el formato %s", fecha, campo, formato)
	}
	return true, nil
}

func ValidarNull(valor string, campo string) (bool, error) {
	if valor == "" {
		return false, fmt.Errorf("el valor del campo %s es nulo", campo)
	}
	return true, nil
}

func ValorMaximo(valor string, valorMaximo string, campo string) (bool, error) {
	num, _ := strconv.ParseFloat(valor, 8)
	max, _ := strconv.ParseFloat(valorMaximo, 8)
	if num > max {
		return false, fmt.Errorf("el valor %s del campo %s supera el valor máximo de %s", valor, campo, valorMaximo)
	}
	return true, nil
}

func ValorMinimo(valor string, valorMinimo string, campo string) (bool, error) {
	num, _ := strconv.ParseFloat(valor, 8)
	min, _ := strconv.ParseFloat(valorMinimo, 8)
	if num < min {
		return false, fmt.Errorf("el valor %s del campo %s es menor al valor mínimo de %s", valor, campo, valorMinimo)
	}
	return true, nil
}

type Event struct {
	Registro   int        `json:"registro"`
	Atributo   string     `json:"atributo"`
	Funcion    []string   `json:"funcion"`
	Valor      string     `json:"valor"`
	Argumentos [][]string `json:"argumentos"`
}

type FuncError struct {
	Registro int    `json:"registro"`
	Atributo string `json:"atributo"`
	Funcion  string
	Error    string
}

type FuncValidation struct {
	Registro int    `json:"registro"`
	Atributo string `json:"atributo"`
	Funcion  string
	Valid    bool `json:"valid"`
}

type Response struct {
	Valido  bool        `json:"valido"`
	Errores []FuncError `json:"errores"`
}

func handler(eventArray []Event) (Response, error) {

	var errores []FuncError
	var validaciones []FuncValidation
	for _, e := range eventArray {

		for i := 0; i < len(e.Funcion); i++ {
			var validation bool
			var err error

			switch e.Funcion[i] {
			case "ValidarNumero":
				validation, err = ValidarNumero(e.Valor, e.Atributo)
			case "LongitudMaxima":
				validation, err = LongitudMaxima(e.Valor, e.Argumentos[i][0], e.Atributo)
			case "LongitudMinima":
				validation, err = LongitudMinima(e.Valor, e.Argumentos[i][0], e.Atributo)
			case "ValidarFormatoFecha":
				validation, err = ValidarFormatoFecha(e.Valor, e.Argumentos[i][0], e.Atributo)
			case "ValidarNull":
				validation, err = ValidarNull(e.Valor, e.Atributo)
			case "ValorMaximo":
				validation, err = ValorMaximo(e.Valor, e.Argumentos[i][0], e.Atributo)
			case "ValorMinimo":
				validation, err = ValorMinimo(e.Valor, e.Argumentos[i][0], e.Atributo)
			case "ValidarCaracteresEspeciales":
				validation, err = ValidarCaracteresEspeciales(e.Valor, e.Atributo)
			case "ValidarDocumento":
				validation, err = ValidarDocumento(e.Valor, e.Argumentos[i][0], e.Atributo)
			case "FormulaIgualdadTexto":
				validation, err = FormulaIgualdadTexto(e.Valor, e.Argumentos[i][0], e.Atributo)
			case "ValoresPosibles":
				validation, err = ValoresPosibles(e.Valor, e.Argumentos[i], e.Atributo)
			case "ValidarFechaMaxima":
				validation, err = ValidarFechaMaxima(e.Valor, e.Argumentos[i][0], e.Argumentos[i][1], e.Atributo)
			case "ValidarFechaMinima":
				validation, err = ValidarFechaMinima(e.Valor, e.Argumentos[i][0], e.Argumentos[i][1], e.Atributo)
			case "FormulaIgualdadNumero":
				validation, err = FormulaIgualdadNumero(e.Valor, e.Argumentos[i][0], e.Argumentos[i][1], e.Atributo)
			default:
				validation = false
				err = fmt.Errorf("Funcion no encontrada")
			}

			if err != nil {
				errores = append(errores, FuncError{Registro: e.Registro, Atributo: e.Atributo, Funcion: e.Funcion[i], Error: err.Error()})
			}
			validaciones = append(validaciones, FuncValidation{Registro: e.Registro, Atributo: e.Atributo, Funcion: e.Funcion[i], Valid: validation})
		}

	}

	fmt.Println(validaciones)

	if len(errores) > 0 {
		return Response{Valido: false, Errores: errores}, nil
	}

	return Response{Valido: true, Errores: errores}, nil
}

func main() {
	lambda.Start(handler)
}
