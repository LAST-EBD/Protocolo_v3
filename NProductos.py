
import os, shutil, re, time, subprocess, pandas, rasterio, sys, urllib, fiona, sqlite3, math, pymongo
import numpy as np
import matplotlib.pyplot as plt
from osgeo import gdal, gdalconst
from datetime import datetime, date

class Product(object):
    
    
    '''Esta clase genera los productos deinundacion, turbidez del agua y ndvi de las escenas normalizadas'''
    
        
    def __init__(self, ruta_nor):
        
        self.ruta_escena = ruta_nor
        self.escena = os.path.split(self.ruta_escena)[1]
        self.raiz = os.path.split(os.path.split(self.ruta_escena)[0])[0]
        print(self.raiz)
        self.nor = os.path.join(self.raiz, os.path.join('nor', self.escena))
        print(self.nor)
        self.ori = os.path.join(self.raiz, os.path.join('ori', self.escena))
        self.data = os.path.join(self.raiz, 'data')
        self.temp = os.path.join(self.raiz, 'temp')
        self.productos = os.path.join(self.raiz, 'pro')
        #self.vals = {}
        #self.d = {}
        self.pro_esc = os.path.join(self.productos, self.escena)
        os.makedirs(self.pro_esc, exist_ok=True)
        
        if 'l8oli' in self.ruta_escena:
            self.sat = 'L8'
        elif 'l7etm' in self.escena:
            self.sat = 'L7'
        elif 'l5tm' in self.ruta_escena:
            self.sat =  'L5'
        elif 'l4tm' in self.ruta_escena:
            self.sat =  'L5'
        else:
            print('No identifico el satelite')
        
        print(self.sat)
        
        if self.sat == 'L8':

            for i in os.listdir(self.nor):
                if re.search('tif$', i):
                    
                    banda = i[-6:-4].lower()
                                        
                    if banda == 'b2':
                        self.blue = os.path.join(self.nor, i)
                    elif banda == 'b3':
                        self.green = os.path.join(self.nor, i)
                    elif banda == 'b4':
                        self.red = os.path.join(self.nor, i)
                    elif banda == 'b5':
                        self.nir = os.path.join(self.nor, i)
                    elif banda == 'b6':
                        self.swir1 = os.path.join(self.nor, i)
                    elif banda == 'b7':
                        self.swir2 = os.path.join(self.nor, i)
                    elif banda == 'k4':
                        self.fmask = os.path.join(self.nor, i)
                    
        else:

            for i in os.listdir(self.nor):
                if re.search('tif$', i):
                    
                    banda = i[-6:-4].lower()
                    print(banda)
                                        
                    if banda == 'b1':
                        self.blue = os.path.join(self.nor, i)
                    elif banda == 'b2':
                        self.green = os.path.join(self.nor, i)
                    elif banda == 'b3':
                        self.red = os.path.join(self.nor, i)
                    elif banda == 'b4':
                        self.nir = os.path.join(self.nor, i)
                    elif banda == 'b5':
                        self.swir1 = os.path.join(self.nor, i)
                    elif banda == 'b7':
                        self.swir2 = os.path.join(self.nor, i)
                    elif banda == 'k4':
                        self.fmask = os.path.join(self.nor, i)
        
        #Insertamos la cobertura de nubes en la BD
        connection = pymongo.MongoClient("mongodb://localhost")
        db=connection.teledeteccion
        landsat = db.landsat
        
        
        try:
        
            landsat.update_one({'_id':self.escena}, {'$set':{'Productos': []}},  upsert=True)
            
        except Exception as e:
            print("Unexpected error:", type(e), e)
            
        print('escena importada para productos correctamente')
        
        
        
    def ndvi(self):

        outfile = os.path.join(self.productos, self.escena + '_ndvi_.tif')
        print(outfile)
        
        with rasterio.open(self.nir) as nir:
            NIR = nir.read()
            #NIR[NIR==0] = 1

        with rasterio.open(self.red) as red:
            RED = red.read()

        num = NIR.astype(float)-RED.astype(float)
        den = NIR+RED
        ndvi = np.true_divide(num, den)
        
        #ndvi[ndvi<-1] = -1
        #ndvi[ndvi>1] = 1
        
        profile = nir.meta
        profile.update(nodata=-9999)
        profile.update(dtype=rasterio.float32)

        with rasterio.open(outfile, 'w', **profile) as dst:
            dst.write(ndvi.astype(rasterio.float32))
            
        #Insertamos la cobertura de nubes en la BD
        connection = pymongo.MongoClient("mongodb://localhost")
        db=connection.teledeteccion
        landsat = db.landsat
        
        
        try:
        
            landsat.update_one({'_id':self.escena}, {'$set':{'Productos': ['NDVI']}},  upsert=True)
            
        except Exception as e:
            print("Unexpected error:", type(e), e)
            
        print('NDVI Generado')
        
        
        
        
    def flood(self):
        
        waterMask = os.path.join(self.data, 'water_mask.tif')
        outfile = os.path.join(self.productos, self.escena + '_flood_1200.tif')
        print(outfile)
        
        with rasterio.open(waterMask) as wmask:
            WMASK = wmask.read()
            #NIR[NIR==0] = 1
            
        with rasterio.open(self.fmask) as fmask:
            FMASK = fmask.read()
            #NIR[NIR==0] = 1

        with rasterio.open(self.swir1) as swir1:
            SWIR1 = swir1.read()
            #RED[RED==0] = 1

        #flood = np.where(((FMASK == 0)| (FMASK == 1)) & (WMASK == 1) & (SWIR1 <= 1636), 1, 0)
        flood = np.where(((FMASK != 2) & (FMASK != 4)) & ((SWIR1 != 0) & (SWIR1 <= 1000)) & (WMASK == 1), 1, 0)
        
        
        profile = swir1.meta
        profile.update(nodata=0)
        profile.update(dtype=rasterio.ubyte)

        with rasterio.open(outfile, 'w', **profile) as dst:
            dst.write(flood.astype(rasterio.ubyte))
            
        #Insertamos la cobertura de nubes en la BD
        connection = pymongo.MongoClient("mongodb://localhost")
        db=connection.teledeteccion
        landsat = db.landsat
        
        
        try:
        
            landsat.update_one({'_id':self.escena}, {'$set':{'Productos': ['Flood']}},  upsert=True)
            
        except Exception as e:
            print("Unexpected error:", type(e), e)
            
        print('Fllod Mask Generada')
