import pandas as pd
import copy
class examen:
    def ejercicio1(self):
        self.Nombres=[]
        self.Characters=[]
        self.comics=pd.read_csv('resources/comics/comics.csv')
        self.characterstocomics = pd.read_csv('resources/comics/charactersToComics.csv')
        self.characters= pd.read_csv('resources/comics/characters.csv')
        self.FilasComics=len(self.comics['comicID'])
        self.FilasCharacterstocomics=len(self.characterstocomics['comicID'])
        self.FilasCharacters = len(self.characters['characterID'])
        for i in range(self.FilasComics):
            print(i)
            self.dato=self.comics.iloc[i][0]
            self.busqueda1=self.characterstocomics['comicID']==self.dato
            self.data1=self.characterstocomics[self.busqueda1]
            for n in range(len(self.data1['characterID'])):
                self.dato2=self.data1.iloc[n][1]
                self.busqueda2=self.characters['characterID']==self.dato2
                self.data2=self.characters[self.busqueda2]
                self.Nombres.append(self.data2.iloc[0][1])
            self.Characters.append(self.Nombres)
            self.Nombres=[]
        self.comics['Characters']=self.Characters
        self.comics.to_csv(r'Comics.csv', index=False)
        self.comics.to_parquet('Comics.parquet', index=False)
    def ejercicio2(self):
        self.Equipos=[]
        self.Equipo = []
        self.Overall=[]
        self.Porteros=[]
        self.Mejores_Porteros=[]
        self.Delanteros=[]
        self.JugadorPort=[]
        self.Overall_Jugador=[]
        self.contador=0
        self.players = pd.read_csv('resources/fifa/players_20.csv')
        self.menoresde23=self.players['age']<23
        self.data=self.players[self.menoresde23]
        self.ordenardata=self.data.sort_values('potential',ascending=False)
        self.datoslistos = self.ordenardata[0:20]
        self.datoslistos.to_csv(r'Potencial_menores_de_23.csv', index=False)
        self.datoslistos.to_parquet('Potencial_menores_de_23.parquet', index=False)
        ####################################################################
        for i in range(len(self.players['club'])):
            self.club=self.players.iloc[i][9]
            for n in range(len(self.Equipos)):
                self.indicador= self.Equipos[n].find(self.club)
                if self.indicador>=0:
                    self.contador=self.contador+1
            if self.contador>0:
                self.contador=0
                continue
            else:
                self.filtroclub=self.players['club']==self.club
                self.data=self.players[self.filtroclub]
                self.overall_equipo=(self.data['overall'].sum())
                self.Equipos.append(self.club)
                self.Overall.append(self.overall_equipo)
        self.equipos_overall=pd.DataFrame({'Equipo': self.Equipos, 'Overall': self.Overall})
        self.datos_ordenados = self.equipos_overall.sort_values('Overall', ascending=False)
        self.mejores_20 = self.datos_ordenados[0:20]
        self.documento_final = copy.deepcopy(self.mejores_20)
        for n in range(len(self.mejores_20)):
            self.filtroequipo = self.players['club'] == self.mejores_20.iloc[n][0]
            self.datosfiltrados=self.players[self.filtroequipo]
            for m in range(len(self.datosfiltrados)):
                self.posiciones = self.datosfiltrados.iloc[m][14]
                self.porteros = self.posiciones.find('GK')
                self.delanteros = self.posiciones.find('ST')
                if self.porteros >= 0:
                    self.Equipo.append(self.club)
                    self.JugadorPort.append(self.datosfiltrados.iloc[m][2])
                    self.Overall_Jugador.append(self.datosfiltrados.iloc[m][10])
            self.porterosdata = pd.DataFrame({'Equipo': self.Equipo, 'Jugador': self.JugadorPort})
            self.porterosdata['Overall'] = self.Overall_Jugador
            self.ordenamiento = self.porterosdata.sort_values('Overall', ascending=False)
            self.mejores_porteros = self.ordenamiento[0:3]
            self.Equipo = []
            self.Overall_Jugador = []
            self.JugadorPort = []
            for c in range(len(self.mejores_porteros)):
                self.seleccion = self.mejores_porteros.iloc[c][1]
                self.JugadorPort.append(self.seleccion)
            self.Mejores_Porteros.append(self.JugadorPort)
            self.JugadorPort = []
            self.Equipo = []
            self.Overall_Jugador = []
        self.documento_final['Mejores_Porteros']=self.Mejores_Porteros
        self.documento_final.to_csv(r'MejoresEquipos.csv', index=False)
        self.documento_final.to_parquet('MejoresEquipos.parquet', index=False)
        #######################################################################
        self.Paises=[]
        self.Jugadores=[]
        self.Jugadores_Final = []
        self.Naciones=[]
        for i in range(len(self.players['nationality'])):
            self.nacionalidad=self.players.iloc[i][8]
            for n in range(len(self.Paises)):
                self.indicador= self.Paises[n].find(self.nacionalidad)
                if self.indicador>=0:
                    self.contador=self.contador+1
            if self.contador>0:
                self.contador=0
                continue
            else:
                self.Paises.append(self.nacionalidad)
                self.filtronacionalidad = self.players['nationality'] == self.nacionalidad
                self.nacionalidad_data = self.players[self.filtronacionalidad]
                self.descendente = self.nacionalidad_data.sort_values('overall', ascending=False)
                self.mejores_rank = self.descendente[0:5]
                self.Naciones.append(self.nacionalidad)
                self.mejores_final = copy.deepcopy(self.mejores_rank)
                for i in range(len(self.mejores_rank)):
                 self.Jugadores.append(self.mejores_final.iloc[i][2])
            self.Jugadores_Final.append(self.Jugadores)
            self.Jugadores=[]
        self.data_mejores = pd.DataFrame(
            {'País': self.Paises,
             '5 Mejores': self.Jugadores_Final
             })
        self.data_mejores.to_csv(r'5Mejoresporpaís.csv', index=False)
        self.data_mejores.to_parquet('5Mejoresporpaís.parquet', index=False)

    def ejercicio3(self):
        self.Tipos=[]
        self.AVG=[]
        self.contador=0
        self.pokemon=pd.read_csv('resources/pokemon/PokemonData.csv')
        for i in range(len(self.pokemon['type1'])):
            self.tipo = self.pokemon.iloc[i][36]
            self.buscar = self.tipo.find('fire')
            self.buscar2 = self.tipo.find('water')
            if self.buscar < 0 and self.buscar2 < 0:
                continue
            else:
                for n in range(len(self.Tipos)):
                    self.indicador = self.Tipos[n].find(self.tipo)
                    if self.indicador >= 0:
                        self.contador = self.contador + 1
                if self.contador > 0:
                    self.contador = 0
                    continue
                else:
                    self.Tipos.append(self.tipo)
                    self.filtrotipo = self.pokemon['type1'] == self.tipo
                    self.tipo_data = self.pokemon[self.filtrotipo]
                    self.avg_generation=(self.tipo_data['generation'].sum())/(len(self.tipo_data['generation']))
                    self.avg_attack=(self.tipo_data['sp_attack'].sum())/(len(self.tipo_data['sp_attack']))
                    self.avg_defense = (self.tipo_data['sp_defense'].sum()) / (len(self.tipo_data['sp_defense']))
                    self.avg_speed = (self.tipo_data['speed'].sum()) / (len(self.tipo_data['speed']))
                    self.AVG.append(self.avg_generation)
                    self.AVG.append(self.avg_attack)
                    self.AVG.append(self.avg_defense)
                    self.AVG.append(self.avg_speed)
        self.datosfinal = pd.DataFrame({'avg_generation_fire': self.AVG[0], 'avg_sp_attack_fire': self.AVG[1],
                                        'avg_sp_defense_fire': self.AVG[2], 'avg_speed_fire': self.AVG[3],
                                        'avg_generation_water': self.AVG[4], 'avg_sp_attack_water': self.AVG[5],
                                        'avg_sp_defense_water': self.AVG[6], 'avg_speed_water': self.AVG[7]
                                        },index=[0])
        print(self.datosfinal)
        self.datosfinal.to_csv(r'PokemonAVG.csv', index=False)
        self.datosfinal.to_parquet('PokemonAVG.parquet', index=False)



