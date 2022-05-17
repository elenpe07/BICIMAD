from pyspark import SparkContext
from collections import Counter
import json
import sys
from pprint import pprint
import statistics
import pandas as pd


sc = SparkContext()


def obtener_dia_edad_tiempo(linea):
	data = json.loads(linea)
	fecha = data['unplug_hourTime'].split('T')[0]
	tiempo = data['travel_time']
	rango_edad = data['ageRange']
	temp = pd.Timestamp(fecha)
	if temp.dayofweek in (0,5):
		i = 0
	else:
		i=1
	return i, rango_edad, tiempo

 
    	
def obtener_dia_edad(linea):
	data=json.loads(linea)
	fecha = data['unplug_hourTime'].split('T')[0]
	rango_edad = data['ageRange']
	temp = pd.Timestamp(fecha)
	if temp.dayofweek in (0,5):
		i = 0
	else:
		i=1
	return i, rango_edad
	
def estaciones(linea):
	data=json.loads(linea)
	est_salida=data['idunplug_station']
	est_llegada=data['idplug_station']
	fecha = data['unplug_hourTime'].split('T')[0]
	temp = pd.Timestamp(fecha)
	if temp.dayofweek in (0,5):
		i = 0
	else:
		i=1
	return i, est_salida, est_llegada
	

def transitadas_semanal(rdd_base):
	rdd_trans_sem1=rdd_base.map(estaciones).filter(lambda x: x[0]==0).map(lambda x : x[1]).collect()
	rdd_trans_sem2=rdd_base.map(estaciones).filter(lambda x: x[0]==0).map(lambda x : x[2]).collect()
	lista=rdd_trans_sem1+rdd_trans_sem2
	contador=Counter(lista)
	print(contador)



def transitadas_findes(rdd_base):
	rdd_trans_sem1=rdd_base.map(estaciones).filter(lambda x: x[0]==1).map(lambda x : x[1]).collect()
	rdd_trans_sem2=rdd_base.map(estaciones).filter(lambda x: x[0]==1).map(lambda x : x[2]).collect()
	lista=rdd_trans_sem1+rdd_trans_sem2
	contador=Counter(lista)
	print(contador)



def estudio_semanal(rdd_base):
	rdd_dias=rdd_base.map(obtener_dia_edad_tiempo).filter(lambda x : x[0] == 0).map(lambda x : (x[1],x[2])).groupByKey().map(lambda x : (x[0],sum(list(x[1]))/len(list(x[1])))).collect() 
	return(rdd_dias)



def estudio_finde(rdd_base):
	rdd_dias=rdd_base.map(obtener_dia_edad_tiempo).filter(lambda x : x[0] == 1).map(lambda x : (x[1],x[2])).groupByKey().map(lambda x : (x[0],sum(list(x[1]))/len(list(x[1])))).collect() 
	return(rdd_dias)


def resta_findes(rdd_base):
	rdd_semana=rdd_base.map(obtener_dia_edad_tiempo).filter(lambda x : x[0] == 0).map(lambda x : (x[1],x[2])).groupByKey().map(lambda x : (x[0],sum(list(x[1]))/len(list(x[1]))))
	rdd_finde=rdd_base.map(obtener_dia_edad_tiempo).filter(lambda x : x[0] == 1).map(lambda x : (x[1],x[2])).groupByKey().map(lambda x : (x[0],sum(list(x[1]))/len(list(x[1]))))
	rdd_union=rdd_semana.union(rdd_finde).groupByKey().map(lambda x: (x[0],abs(list(x[1])[0]-list(x[1])[1]))).collect()
	return(rdd_union)
	
def contador(rdd_base):
	rdd_contador_diario=rdd_base.map(obtener_dia_edad).filter(lambda x: x[0]==0).map(lambda x: x[1]).collect()
	contadordiario=Counter(rdd_contador_diario)
	rdd_contador_findes=rdd_base.map(obtener_dia_edad).filter(lambda x: x[0]==1).map(lambda x: x[1]).collect()
	contadorfindes=Counter(rdd_contador_findes)
	print("\nContador de viajes según el rango de edad DIAS DE DIARIO\n")
	print(contadordiario)
	print("\nContador de viajes segun el rango de edad FINES DE SEMANA\n")
	print(contadorfindes)
	print("\nDiferencia entre la cantidad de viajes realizados según la edad LOS FINES DE SEMANA Y ENTRE SEMANA\n")
	a=list(contadordiario.values())
	b=list(contadorfindes.values())
	print([abs(e1 - e2) for e1, e2 in zip(a,b)])
	


def proceso(rdd):
    
    print("Media del tiempo de viaje en base a la edad ENTRE SEMANA\n")
    print(estudio_semanal(rdd))
    print("\nMedia del tiempo de viaje en base a la edad LOS FINES DE SEMANA\n")
    print(estudio_finde(rdd))
    print("\nResta de medias de tiempo según la edad entre fines de semana y días de la semana\n")
    print(resta_findes(rdd))
    print(contador(rdd))
    print("\n Contador estaciones más transitadas entre semana\n")
    print(transitadas_semanal(rdd))
    print("\n Contador estaciones más transitadas durante los fines de semana\n")
    print(transitadas_findes(rdd))
	
def main():
    rdd = sc.parallelize([])
    rdd=rdd.union(sc.textFile("202102_movements.json"))
    proceso(rdd)



if __name__ =="__main__":
    main()
    






