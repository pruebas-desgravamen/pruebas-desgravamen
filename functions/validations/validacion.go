package main

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
)

func roundFloat(val float64, precision int) float64 {
	ratio := math.Pow(10, float64(precision))
	return math.Round(val*ratio) / ratio
}

func ValidarNumero(valor string) (bool, error) {
	_, err := strconv.ParseFloat(valor, 64)
	if err != nil {
		return false, fmt.Errorf("el valor no es un número")
	}
	return true, nil
}

func ValoresPosibles(valor string, valoresPosibles []string) (bool, error) {
	for _, valorPosible := range valoresPosibles {
		if valor == valorPosible {
			return true, nil
		}
	}
	return false, fmt.Errorf("el valor no es uno de los valores posibles")
}

func LongitudMaxima(valor string, longitud int) (bool, error) {
	if len(valor) > longitud {
		return false, fmt.Errorf("el valor %s supera la longitud máxima de %d", valor, longitud)
	}
	return true, nil
}

func ValidarCaracteresEspeciales(texto string) (bool, error) {
	for _, letra := range texto {
		if (letra < 'a' || letra > 'z') && (letra < 'A' || letra > 'Z') {
			return false, fmt.Errorf("el texto no es valido")
		}
	}
	return true, nil
}

func FormulaIgualdadNumero(valor string, decimales string, valorComparacion string) (bool, error) {

	num1, err := strconv.ParseFloat(valor, 32)

	if err != nil {
		return false, err
	}

	num2, err := strconv.ParseFloat(valorComparacion, 32)

	if err != nil {
		return false, err
	}

	dec, err := strconv.Atoi(decimales)

	if err != nil {
		return false, err
	}

	if roundFloat(num1, dec) == roundFloat(num2, dec) {
		return true, nil
	}

	return false, fmt.Errorf("los valores no son iguales")

}

func FormulaIgualdadTexto(valor string, valorComparacion string) (bool, error) {

	if valor == valorComparacion {
		return true, nil
	}

	return false, fmt.Errorf("los valores no son iguales")

}

func LongitudMinima(texto string, longitud int) (bool, error) {
	if len(texto) < longitud {
		return false, fmt.Errorf("el valor %s supera la longitud mínima de %d", texto, longitud)
	}
	return true, nil
}

func ValidarDocumento(documento string, tipoDocumento string) (bool, error) {
	var longitud int
	tipo, _ := strconv.Atoi(tipoDocumento)

	switch tipo {
	case 2:
		longitud = 32

	default:
		longitud = 10
	}
	if len(documento) == longitud {
		return true, nil
	}

	return false, fmt.Errorf("el documento no es valido")
}

func ValidarFechaMaxima(fecha string, fechaComparacion string, formato string) (bool, error) {
	fechaCmp, err := time.Parse(formato, fechaComparacion)
	if err != nil {
		return false, err
	}

	fechaGo, err := time.Parse(formato, fecha)

	if err != nil {
		return false, err
	}

	if fechaGo.After(fechaCmp) {
		return false, fmt.Errorf("fecha %s es mayor a %s", fecha, fechaComparacion)
	}

	return true, nil

}

func ValidarFechaMinima(fecha string, fechaComparacion string, formato string) (bool, error) {
	fechaCmp, err := time.Parse(formato, fechaComparacion)
	if err != nil {
		return false, err
	}

	fechaGo, err := time.Parse(formato, fecha)

	if err != nil {
		return false, err
	}

	if fechaGo.Before(fechaCmp) {
		return false, fmt.Errorf("fecha %s es menor a %s", fecha, fechaComparacion)
	}

	return true, nil

}

func ValidarFormatoFecha(fecha string, formato string) (bool, error) {
	_, err := time.Parse(formato, fecha)
	if err != nil {
		return false, fmt.Errorf("fecha %s no cumple con el formato %s", fecha, formato)
	}
	return true, nil
}

func ValidarNull(valor string) (bool, error) {
	if valor == "" {
		return false, fmt.Errorf("el valor no puede ser nulo")
	}
	return true, nil
}

func ValorMaximo(valor string, valorMaximo string) (bool, error) {
	num, _ := strconv.ParseFloat(valor, 32)
	max, _ := strconv.ParseFloat(valorMaximo, 32)
	if num > max {
		return false, fmt.Errorf("el valor es mayor al valor maximo")
	}
	return true, nil
}

func ValorMinimo(valor string, valorMinimo string) (bool, error) {
	num, _ := strconv.ParseFloat(valor, 32)
	min, _ := strconv.ParseFloat(valorMinimo, 32)
	if num < min {
		return false, fmt.Errorf("el valor es menor al valor minimo")
	}
	return true, nil
}

func Handler(funcion string, argumentos string) (bool, error) {
	args := strings.Split(argumentos, ",")
	var res bool
	var err error

	switch funcion {
	case "ValidarNumero":
		res, err = ValidarNumero(args[0])
	case "ValoresPosibles":
		res, err = ValoresPosibles(args[0], args[1:])
	case "LongitudMaxima":
		long, _ := strconv.Atoi(args[1])
		res, err = LongitudMaxima(args[0], long)
	case "ValidarCaracteresEspeciales":
		res, err = ValidarCaracteresEspeciales(args[0])
	case "FormulaIgualdadNumero":
		res, err = FormulaIgualdadNumero(args[0], args[1], args[2])
	case "FormulaIgualdadTexto":
		res, err = FormulaIgualdadTexto(args[0], args[1])
	case "ValidarFechaMaxima":
		res, err = ValidarFechaMaxima(args[0], args[1], args[2])
	case "ValidarFechaMinima":
		res, err = ValidarFechaMinima(args[0], args[1], args[2])
	case "ValidarFormatoFecha":
		res, err = ValidarFormatoFecha(args[0], args[1])
	case "ValidarNull":
		res, err = ValidarNull(args[0])
	case "ValorMaximo":
		res, err = ValorMaximo(args[0], args[1])
	case "ValorMinimo":
		res, err = ValorMinimo(args[0], args[1])
	default:
		return false, fmt.Errorf("funcion no encontrada")

	}
	return res, err
}

func main() {
	lambda.Start(Handler)
}
