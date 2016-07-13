from pymongo import MongoClient
import MySQLdb
import decimal
from decimal import *
from datetime import date


def conexion():
	db = MySQLdb.connect(host="localhost", user="user",passwd="pass", db="public")
	cursor = db.cursor()
	return cursor
	
def nameTables(base):#regresa un arreglo con el nombre de las tablas de la base de datos
	tablas=[]
	cursor=conexion()
	cursor.execute( "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA LIKE '%"+base+"%'")#obtengo el nombre de las tablas de la base
	for tabla in cursor:#recorre esas tuplas con los nombres de todas las tablas
		tablas.append(tabla[0])
	return tablas
		
def nameColumnas(base):#se obtienen los nombres de las columnas de las tablas de la base de datos
	tablas = nameTables(base)
	cursor=conexion()
	for tabla in tablas:
		columnas = []
		cursor.execute( "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA LIKE '%"+base+"%' AND TABLE_NAME = '"+tabla+"' ")#una vez que obtengo el nombre de la tabla 
		for col in cursor:
			columnas.append(col[0])
		val = valores(tabla)#se obtienen los valores de cada tabla
		arr=crearDict(columnas,val)#se crea el diccionario con los nombres de las columnas y los valores 
		insertMongo(arr,tabla)#en esta funcion se inserta en la base de datos mongo
		val=None
		arr=None
		columnas=None

def valores(nombreTabla):
	valores=[]
	cursor=conexion()
	cursor.execute("SELECT * FROM  "+nombreTabla+";")#cursor regresa los valores de una tabla 
	for val in cursor:
		valores.append(val)
	return valores
	
	
			
def crearDict(columnas,valores):
	arrJson = []
	for val in valores:

		diccionario = dict.fromkeys(columnas)
		for i in range(len(columnas)):
			#print(type(val[i]))
			if isinstance(val[i],Decimal):#problemas con los decimal en mongo 
				value = float(val[i])#se convierten a Float
			elif isinstance(val[i],str):#problemas con los string
				value = val[i].decode('ascii','ignore')
			elif isinstance(val[i],date):
				value = str(val[i])
			else :
				value= val[i]
			diccionario[columnas[i]] = value
		arrJson.append(diccionario)
		diccionario = None
	return arrJson 


def insertMongo(arreglo,nombCollect):
	client = MongoClient()
	db = client.datos#datos es el nombre de la base
	insert = "db."+nombCollect+".insert(arreglo)"
	#nombCollect es el nombre de la collection
	print insert
	exec insert#'''
		
	
if __name__ =="__main__":
	nameColumnas("public")#Nombre de la base de datos
	