package main

import "testing"
import "github.com/stretchr/testify/assert"

func TestCalculateCronString1(t *testing.T) {
	cs := CalculateCronString(
						[]string{"1","L"},//second
						[]string{"0","L"},//minute
						[]string{"1","L"},//hour
						[]string{"1","L"},//day
						[]string{"0","L"},//week
						[]string{"1","L"},//month
						[]string{"1","L"})//year

	assert.Equal(t, "* * * * * * *", cs, "cron string")
}

func TestCalculateCronString2(t *testing.T) {
	cs := CalculateCronString(
						[]string{"0","L"},//second
						[]string{"0","L"},//minute
						[]string{"0","L"},//hour
						[]string{"1","L"},//day
						[]string{"1","L"},//week
						[]string{"1","L"},//month
						[]string{"1","L"})//year

	assert.Equal(t, "59 59 23 * * * *", cs, "cron string")
}

func TestCalculateCronString3(t *testing.T) {
	cs := CalculateCronString(
						[]string{"0","L"},//second
						[]string{"0","22"},//minute
						[]string{"0","33"},//hour
						[]string{"458","4"},//day
						[]string{"1","L"},//week
						[]string{"1","L"},//month
						[]string{"1","L"})//year

	assert.Equal(t, "22 33 4 * * * *", cs, "cron string")
}

func TestCalculateCronString4(t *testing.T) {
	cs := CalculateCronString(
						[]string{"0","L"},//second
						[]string{"0","22"},//minute
						[]string{"0","L"},//hour
						[]string{"0","7"},//day
						[]string{"4","L"},//week
						[]string{"1","L"},//month
						[]string{"1","L"})//year

	// Seconds      Minutes      Hours      Day Of Month      Month      Day Of Week      Year
	assert.Equal(t, "22 59 7 * * SAT *", cs, "cron string")
}


func TestCalculateCronString5(t *testing.T) {
	cs := CalculateCronString(
						[]string{"0","L"},//second
						[]string{"0","22"},//minute
						[]string{"0","L"},//hour
						[]string{"0","7"},//day
						[]string{"44","L"},//week
						[]string{"0","L"},//month
						[]string{"45","L"})//year

	assert.Equal(t, "22 59 7 * * SAT *", cs, "cron string")
}

func TestCalculateCronString6(t *testing.T) {
	cs := CalculateCronString(
						[]string{"0","L"},//second
						[]string{"0","22"},//minute
						[]string{"0","L"},//hour
						[]string{"0","7"},//day
						[]string{"0","L"},//week
						[]string{"2","10"},//month
						[]string{"45","L"})//year

	assert.Equal(t, "22 59 7 10 * * *", cs, "cron string")
}


func TestCalculateCronString7(t *testing.T) {
	cs := CalculateCronString(
						[]string{"0","L"},//second
						[]string{"0","22"},//minute
						[]string{"0","L"},//hour
						[]string{"0","7"},//day
						[]string{"0","L"},//week
						[]string{"0","L"},//month
						[]string{"0","L"})//year

	assert.Equal(t, "22 59 7 L 12 * *", cs, "cron string")
}


