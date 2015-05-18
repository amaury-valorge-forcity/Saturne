#!/usr/bin/env python
# -*-coding:Utf-8 -*

import sys
import atexit, sys
from grass.script import parser, run_command
import grass.script as grass
import os.path
import subprocess
from math import *
import time

''' Suppression fichiers temporaires '''
def clean(self):
	for tmp in self:
		grass.run_command('g.remove', flags ="fb", type='vector', pattern=tmp, quiet=True)
		
''' traitement '''
def main():
	''' acces BDD PostGIS'''
	BDDNAME='saturne'
	IP='92.222.75.150'
	LOGIN='AmauryValorge'
	MDP='z9pY1Pm6dKaTuNfwMbSj'
	
	''' importation '''
	# .message("Importation du reseau RCU...")
	# SCHEM='topology'
	# COUCHE='linear'
	# sql="source = 'LIGNES_CONSTRUCTIBLES_2015_05_11' AND id NOT LIKE 'TRONROUT%'" 
	# grass.run_command('v.in.ogr', input='PG:dbname=' + BDDNAME + ' host=' + IP + ' port=5432' + ' user=' + LOGIN + ' password=' + MDP + ' sslmode=require' + ' schemas=' + SCHEM, output='ORI_rsxRCU', where=sql, layer=COUCHE, quiet=True)

	# grass.message("Importation du reseau routier...")
	# SCHEM='topology'
	# COUCHE='linear'
	# sql="source = 'LIGNES_CONSTRUCTIBLES_2015_05_11' AND id LIKE 'TRONROUT%'" 
	# grass.run_command('v.in.ogr', input='PG:dbname=' + BDDNAME + ' host=' + IP + ' port=5432' + ' user=' + LOGIN + ' password=' + MDP + ' sslmode=require' + ' schemas=' + SCHEM, output='ORI_rsxRTE', where=sql, layer=COUCHE, quiet=True)
		
	# ORI_enveloppebati - ne pas nettoyer la geometrie

	# grass.message("Importation des chaufferies...")
	# SCHEM='topology'
	# COUCHE='punctual'
	# sql="source = 'SAISIE_MANUELLE_DALKIA_FORCITY'"
	# grass.run_command('v.in.ogr', input='PG:dbname=' + BDDNAME + ' host=' + IP + ' port=5432' + ' user=' + LOGIN + ' password=' + MDP + ' sslmode=require' + ' schemas=' + SCHEM, output='ORI_Chauff', where=sql, layer=COUCHE, quiet=True)
		
	''' Creation des couches finales '''
	grass.message("Preparation des couches...")
	grass.run_command('g.copy', vector=('ORI_enveloppebati','TMPPP_bati'), quiet=True) #enveloppes bti !
	# non utile a priori -  grass.run_command('v.clean', flags='c', input="ORI_bati", output='TMPPP_bati_full', tool='break', quiet=True)
	# grass.run_command('v.clean', flags='c', input="ORI_rsxRCU", output='TMPPP_rsx_RCU', tool='break', quiet=True)
	# grass.run_command('v.clean', flags='c', input="ORI_rsxRTE", output='TMPPP_rsx_ROUTES', tool='break', quiet=True)
	grass.run_command('g.copy', vector=('ORI_Chauff','TMPP_Chauff'), quiet=True)

	''' recuperation des distances '''
	grass.message("calcul des distances...")
	grass.run_command('v.db.addcolumn', map='TMPP_Chauff', columns='BATIID varchar(254),RSXRCU DOUBLE PRECISION,RSXRTETP DOUBLE PRECISION,RSXRTE DOUBLE PRECISION,DISTBATI DOUBLE PRECISION', quiet=True)
	grass.read_command('v.distance', _from='TMPP_Chauff', from_type='point', to='TMPPP_bati', to_type='area', upload='to_attr', to_colum='id', column='BATIID', quiet=True)
	grass.read_command('v.distance', _from='TMPP_Chauff', from_type='point', to='TMPPP_bati', to_type='area', upload='dist', column='DISTBATI', quiet=True)
	grass.read_command('v.distance', _from='TMPP_Chauff', from_type='point', to='TMPPP_rsx_RCU', to_type='line', upload='dist', column='RSXRCU', quiet=True)
	grass.read_command('v.distance', _from='TMPP_Chauff', from_type='point', to='TMPPP_rsx_ROUTES', to_type='line', upload='dist', column='RSXRTETP', quiet=True)
	grass.run_command('v.db.update', map='TMPP_Chauff', column='RSXRTE', query_column='RSXRTETP+32', quiet=True)
	grass.run_command('v.db.dropcolumn', map='TMPP_Chauff', columns='RSXRTETP', quiet=True)
	
	''' exctraction selon criteres '''
	ListChauff=[]
	grass.message("extraction...")
	expr='RSXRCU<RSXRTE'
	grass.run_command('v.extract', input='TMPP_Chauff', output='ORI_Chauff_RCU', where=expr, quiet=True)
	grass.run_command('v.db.renamecolumn', map='ORI_Chauff_RCU', column=('RSXRCU','DISTRSX'), quiet=True)
	grass.run_command('v.db.dropcolumn', map='ORI_Chauff_RCU', columns='RSXRTE', quiet=True)
	infopt = grass.vector_info_topo('ORI_Chauff_RCU')
	if infopt['points'] > 0:
		ListChauff.append('ORI_Chauff_RCU')

	grass.run_command('v.extract', flags='r', input='TMPP_Chauff', output='ORI_Chauff_RTE', where=expr, quiet=True)
	grass.run_command('v.db.renamecolumn', map='ORI_Chauff_RTE', column=('RSXRTE','DISTRSX'), quiet=True)	
	grass.run_command('v.db.dropcolumn', map='ORI_Chauff_RTE', columns='RSXRCU', quiet=True)
	infopt = grass.vector_info_topo('ORI_Chauff_RTE')
	if infopt['points'] > 0:
		ListChauff.append('ORI_Chauff_RTE')

	''' verification des connections possibles '''
	grass.message("finalisation...")
	for layer in ListChauff:
		expr='DISTBATI>DISTRSX'
		grass.run_command('v.extract', input=layer, output='TMPP_NetSimple_' + layer, where=expr, quiet=True)
		grass.run_command('v.extract', flags='r', input=layer, output='TMPPP_NetProcess_' + layer, where=expr, quiet=True)
	
	infopt = grass.vector_info_topo('TMPP_NetSimple_ORI_Chauff_RCU')
	if infopt['points'] > 0:
		grass.read_command('v.distance', flags='p', _from='TMPP_NetSimple_ORI_Chauff_RCU', from_type='point', to='TMPPP_rsx_RCU', to_type='line', upload='dist', output='TP_NetSimple_ORI_Chauff_CONNECT_RCU', quiet=True)
		grass.run_command('v.db.addtable', map='TP_NetSimple_ORI_Chauff_CONNECT_RCU', columns="source varchar(255), id varchar(255), valid_time varchar(255)", quiet=True)
		grass.run_command('v.db.update', map='TP_NetSimple_ORI_Chauff_CONNECT_RCU', column='source', value='RACCsimpleRCU_FORCITY', quiet=True)
	
	infopt = grass.vector_info_topo('TMPP_NetSimple_ORI_Chauff_RTE')
	if infopt['points'] > 0:		
		grass.read_command('v.distance', flags='p', _from='TMPP_NetSimple_ORI_Chauff_RTE', from_type='point', to='TMPPP_rsx_ROUTES', to_type='line', upload='dist', output='TP_NetSimple_ORI_Chauff_CONNECT_RTE', quiet=True)
		grass.run_command('v.db.addtable', map='TP_NetSimple_ORI_Chauff_CONNECT_RTE', columns="source varchar(255), id varchar(255), valid_time varchar(255)", quiet=True)
		grass.run_command('v.db.update', map='TP_NetSimple_ORI_Chauff_CONNECT_RTE', column='source', value='RACCsimpleRTE_FORCITY', quiet=True)
	
	dvectNetSimple = grass.parse_command('g.list', type="vect", pattern="TP_NetSimple_ORI_Chauff_CONNECT_*", quiet=True)
	grass.run_command('v.edit', tool='create', map='TP_Raccord_NetSimple_Finale', quiet=True)
	grass.run_command('v.db.addtable', map='TP_Raccord_NetSimple_Finale', columns="source varchar(255), id varchar(255), valid_time varchar(255)", quiet=True)
	for vct in dvectNetSimple:
		grass.run_command('v.patch', flags='ea', input=vct, output='TP_Raccord_NetSimple_Finale', quiet=True, overwrite=True)
	grass.run_command('v.db.update', map='TP_Raccord_NetSimple_Finale', column='source', value='RACC_FORCITY', quiet=True)
	grass.run_command('v.db.update', map='TP_Raccord_NetSimple_Finale', column='id', value='RACC_DIRECT', quiet=True)
		
	tempfile = ['TMPP_*']
	clean(tempfile)

	
	
	# ''' connection complexe '''
	# ''' Creation de la couche finale '''
	listerrorRCU=[]
	listerrorRTE=[]
	grass.message("Preparation des couches...")
	grass.run_command('v.patch', flags='e', input=('TMPPP_NetProcess_ORI_Chauff_RCU','TMPPP_NetProcess_ORI_Chauff_RTE'), output='TMPPP_NetProcess', quiet=True)

	grass.run_command('v.db.addcolumn', map='TMPPP_NetProcess', columns='NUM INTEGER', quiet=True)
	valuecat=grass.parse_command('v.category', input='TMPPP_NetProcess', type='point', option='print')
	newcat=1
	for f in valuecat:
		grass.write_command("db.execute", input="-", stdin="update TMPPP_NetProcess SET NUM='{0}' WHERE cat='{1}'".format(newcat,str(f)), quiet=True)
	 	newcat+=1
	
	grass.run_command('v.edit', tool='create', map='TP_Raccord_NetProcess_Finale', quiet=True)
	grass.run_command('v.db.addtable', map='TP_Raccord_NetProcess_Finale', columns="source varchar(255), id varchar(255), valid_time varchar(255)", quiet=True)

	infopt = grass.vector_info_topo('TMPPP_NetProcess')
	objpt = 1
	while objpt <= infopt['points']:
		grass.message("\nPoint N. {0} / {1}" .format(objpt,infopt['points'])) 
		
		grass.message("extraction des chaufferies...")
		expr='NUM='+str(objpt)
		grass.run_command('v.extract', input='TMPPP_NetProcess', output='TMP_NetProcess_pt'+str(objpt), where=expr, quiet=True)
		
		grass.message("extraction du batiment correspondant et retraitement...")
		catbati=grass.read_command('v.db.select', flags='cv', map='TMP_NetProcess_pt'+str(objpt), columns='BATIID', quiet=True)
		attbati=catbati.split()[0]
		expr="id='"+str(attbati)+"'"
		# grass.run_command('v.extract', input='TMPPP_bati', output='TMP_NetProcess_bati_extract'+str(objpt), where=expr, quiet=True)
		grass.run_command('v.extract', input='TMPPP_bati', output='TMP_NetProcess_bati_ext'+str(objpt), where=expr, quiet=True)
		grass.run_command('v.centroids', input='TMP_NetProcess_bati_ext'+str(objpt), output='TMP_NetProcess_bati_ext_centro'+str(objpt), quiet=True)
		grass.run_command('v.category', input='TMP_NetProcess_bati_ext_centro'+str(objpt), output='TMP_NetProcess_bati_ext_centro_catego'+str(objpt), option='add', quiet=True)
		grass.run_command('v.extract', flags='d', input='TMP_NetProcess_bati_ext_centro_catego'+str(objpt), output='TMP_NetProcess_bati_extract'+str(objpt), new='1', quiet=True)

		
		grass.message("extraction des coordonnees du batiment correspondant...")
		coordbati=grass.read_command('v.info', flags='g', map='TMP_NetProcess_bati_extract'+str(objpt), quiet=True)
		dictcoordbati= dict( (n,str(v)) for n,v in (a.split('=') for a in coordbati.split() ) )
		
		
		grass.message("calcul de la distance du reseau le plus proche...")
		distrsxRTE = grass.read_command('v.distance', flags='p', _from='TMP_NetProcess_pt'+str(objpt), from_type='point', to='TMPPP_rsx_ROUTES', to_type='line', upload='dist', quiet=True)
		distancersxRTE=[]
		for i in distrsxRTE.split():
			distancersxRTE.append(i.split('|'))
		distancersxRTE.remove(distancersxRTE[0])
		distanceRTE = ceil(float(distancersxRTE[0][1]))
		
		distrsxRCU = grass.read_command('v.distance', flags='p', _from='TMP_NetProcess_pt'+str(objpt), from_type='point', to='TMPPP_rsx_RCU', to_type='line', upload='dist', quiet=True)
		distancersxRCU=[]
		for i in distrsxRCU.split():
			distancersxRCU.append(i.split('|'))
		distancersxRCU.remove(distancersxRCU[0])
		distanceRCU = ceil(float(distancersxRCU[0][1]))
		
		''' On recherche la coordonnes la plus proche par rapport aux deux reseaux ; la valeur 10 est le decalage possible # CORRECTION +60m car les chauff loin des batiment sont pas pris en compte
		le reseau peux ne pas etre pris par l'overlay ensuite de la region calculee ci dessus... dou 60m.. '''
		if (distanceRCU <= distanceRTE+32): 
			distance = distanceRCU+32
		else:
			distance = distanceRTE+32
			
		grass.message("calage de la region sur l'emprise du batiment correspondant...")
		grass.run_command('g.region', flags='a', n=float(dictcoordbati['north'])+distance, s=float(dictcoordbati['south'])-distance, e=float(dictcoordbati['east'])+distance, w=float(dictcoordbati['west'])-distance, quiet=True)
		grass.run_command('v.in.region', output='TMP_NetProcess_region_L'+str(objpt), quiet=True)
		
		grass.message("extraction du reseau sur la region de travail...")
		if (distanceRCU <= distanceRTE+32): # RCU == 'ok':
			grass.message("RCU - conversion ligne du reseau vers point et calcul du cout...")
			grass.run_command('v.overlay', flags='t', ainput='TMPPP_rsx_RCU', atype='line', binput='TMP_NetProcess_region_L'+str(objpt), output='TMP_NetProcess_rsx_RCU'+str(objpt), operator='and', quiet=True)
			grass.run_command('v.to.points', input='TMP_NetProcess_rsx_RCU'+str(objpt), output='TMP_NetProcess_rsx_RCU_pt'+str(objpt), dmax='1', quiet=True)
			grass.run_command('g.copy', vector=('TMP_NetProcess_rsx_RCU_pt'+str(objpt),'TMP_NetProcess_rsx_pt'+str(objpt)), quiet=True)
			typeracc = 'RCU'
		else:
			grass.message("ROUTES - conversion ligne du reseau vers point et calcul du cout...")
			grass.run_command('v.overlay', flags='t', ainput='TMPPP_rsx_ROUTES', atype='line', binput='TMP_NetProcess_region_L'+str(objpt), output='TMP_NetProcess_rsx_ROUTES'+str(objpt), operator='and', quiet=True)
			grass.run_command('v.to.points', input='TMP_NetProcess_rsx_ROUTES'+str(objpt), output='TMP_NetProcess_rsx_ROUTES_pt'+str(objpt), dmax='1', quiet=True)
			grass.run_command('g.copy', vector=('TMP_NetProcess_rsx_ROUTES_pt'+str(objpt),'TMP_NetProcess_rsx_pt'+str(objpt)), quiet=True)
			typeracc = 'ROUTES'
		
		grass.message("assemblage des bati point et du reseau point pour un maillage de point complet...")
		grass.run_command('v.to.points', input='TMP_NetProcess_bati_extract'+str(objpt), output='TMP_NetProcess_region_bati_pt'+str(objpt), type='area', dmax='1', quiet=True) 
		grass.run_command('v.patch', input=('TMP_NetProcess_region_bati_pt'+str(objpt),'TMP_NetProcess_rsx_pt'+str(objpt)), output='TMP_NetProcess_pt_bati_rsx'+str(objpt), quiet=True)
		
		grass.message("creation du diagramme de voronoi...")
		grass.run_command('v.voronoi', flags='l', input='TMP_NetProcess_pt_bati_rsx'+str(objpt), output='TMP_NetProcess_pt_bati_rsx_voro'+str(objpt), quiet=True)
			
		grass.message("suppression des lignes du voronoi a linterieur de chaque bati...")
		grass.run_command('v.overlay', flags='t', ainput='TMP_NetProcess_pt_bati_rsx_voro'+str(objpt), atype='line', binput='TMP_NetProcess_bati_extract'+str(objpt), output='TMP_NetProcess_voroNot_'+str(objpt), operator='not', quiet=True)
		
		grass.message("prise en compte des autres batiments...")
		grass.run_command('v.select', ainput='TMPPP_bati', atype='area', binput="TMP_NetProcess_region_L"+str(objpt), btype='area', output='TMP_NetProcess_bati_select'+str(objpt), operator='overlap', quiet=True)
		''' grass.run_command('v.clean', flags='c', input="TMP_NetProcess_bati_select"+str(objpt), output='TMP_NetProcess_bati_select_cl'+str(objpt), tool='snap,break,bpol', type='boundary', threshold='1', quiet=True)'''
		'''fusion''' # verif
		grass.run_command('v.extract', flags='r', input='TMP_NetProcess_bati_select'+str(objpt), output='TMP_NetProcess_bati_select_cl_buff_fusio'+str(objpt), where=expr, quiet=True)
		# grass.run_command('v.extract', flags='d', input='TMP_NetProcess_bati_select_cl_buff_ext'+str(objpt), output='TMP_NetProcess_bati_select_cl_buff_fusio'+str(objpt), new='1', quiet=True)

		grass.message("suppression graph voro dans autre bati...")
		grass.run_command('v.overlay', flags='t', ainput='TMP_NetProcess_voroNot_'+str(objpt), atype='line', binput='TMP_NetProcess_bati_select_cl_buff_fusio'+str(objpt), output='TMP_NetProcess_voroNot_bis'+str(objpt), operator='not', quiet=True)
		
		grass.message("conversion du bati en ligne avec voro pour integration dans la couche voronoi et nettoyage...")
		grass.run_command('v.type', input='TMP_NetProcess_bati_extract'+str(objpt), output='TMP_NetProcess_region_bati_buff_line'+str(objpt), from_type='boundary', to_type='line', quiet=True)
		''' integration des autres bati '''
		grass.run_command('v.type', input='TMP_NetProcess_bati_select_cl_buff_fusio'+str(objpt), output='TMP_NetProcess_bati_select_cl_buff_line'+str(objpt), from_type='boundary', to_type='line', quiet=True)
		''' on ne veux pas des lignes a linterieur du polygone du bati retrace - idem raccord enfin uniquement dans les bati.. '''  #useless
		grass.run_command('v.overlay', flags='t', ainput='TMP_NetProcess_bati_select_cl_buff_line'+str(objpt), atype='line', binput='TMP_NetProcess_bati_extract'+str(objpt), output='TMP_NetProcess_bati_select_cl_buff_line_not'+str(objpt), operator='not', quiet=True)
		
		''' integration des raccords existants... '''
		inforacc = grass.vector_info_topo('TP_Raccord_NetProcess_Finale')
		if inforacc['lines'] == 0:
			grass.run_command('v.patch', input=('TMP_NetProcess_voroNot_bis'+str(objpt),'TMP_NetProcess_region_bati_buff_line'+str(objpt),'TMP_NetProcess_bati_select_cl_buff_line_not'+str(objpt)), output='TMP_NetProcess_voroNot_bati_line'+str(objpt), quiet=True)
		else:
			grass.run_command('v.extract', input='TP_Raccord_NetProcess_Finale', type='line', output='TMP_NetProcess_Raccord'+str(objpt), quiet=True)
			grass.run_command('v.patch', input=('TMP_NetProcess_voroNot_bis'+str(objpt),'TMP_NetProcess_region_bati_buff_line'+str(objpt),'TMP_NetProcess_Raccord'+str(objpt),'TMP_NetProcess_bati_select_cl_buff_line_not'+str(objpt)), output='TMP_NetProcess_voroNot_bati_line'+str(objpt), quiet=True)

		grass.run_command('v.clean', flags='c', input="TMP_NetProcess_voroNot_bati_line"+str(objpt), output='TMP_NetProcess_voroNot_bati_line_cl'+str(objpt), tool='snap,break', type='area', threshold='0.1', quiet=True)

		''' connection avec le bati et les chaufferie '''
		grass.message("generation du 1er reseau...")
		grass.run_command('v.net', flags='c', input='TMP_NetProcess_voroNot_bati_line_cl'+str(objpt), points='TMP_NetProcess_pt'+str(objpt), output='TMP_NetProcess_voroNot_bati_line_cl_PT_cnct_'+str(objpt), operation='connect', threshold='10000', quiet=True)
		grass.run_command('v.clean', flags='c', input="TMP_NetProcess_voroNot_bati_line_cl_PT_cnct_"+str(objpt), output='TMP_NetProcess_voroNot_bati_line_cl_PT_cnct_cl'+str(objpt), tool='break', quiet=True)
		
		''' connection avec le bati + raccord chaufferie ET le reseau en PT '''
		grass.message("generation du 2nd reseau...")
		grass.run_command('v.net', flags='c', input='TMP_NetProcess_voroNot_bati_line_cl_PT_cnct_cl'+str(objpt), points='TMP_NetProcess_rsx_pt'+str(objpt), output='TMP_NetProcess_voroNot_bati_line_cl_PT_cnct_cl_rsx_cnct'+str(objpt), operation='connect', node_layer='3', threshold='10000', quiet=True)
		
		''' nettoyage du reseau '''
		grass.message("nettoyage du reseau...")
		grass.run_command('v.clean', flags='c', input="TMP_NetProcess_voroNot_bati_line_cl_PT_cnct_cl_rsx_cnct"+str(objpt), output='TMP_NetProcess_voroNot_bati_line_cl_PT_cnct_cl_rsx_cnct_cl'+str(objpt), tool='snap,break', threshold='0.1', quiet=True)
		
		''' calcul du chemin le plus court '''
		grass.message("Calcul du chemin le plus court...") # a priori pas besoin de cout pour ligne..
		grass.run_command('v.net.distance', input='TMP_NetProcess_voroNot_bati_line_cl_PT_cnct_cl_rsx_cnct_cl'+str(objpt), output='TMP_NetProcess_'+str(objpt), from_layer='2', to_layer='3', quiet=True)
		
		'''  remplissage des attributs '''
		grass.message("Remplissage des attributs...")
		grass.run_command('v.db.droptable', flags ='f', map='TMP_NetProcess_'+str(objpt), quiet=True)
		grass.run_command('v.db.addtable', map='TMP_NetProcess_'+str(objpt), columns="source varchar(255), id varchar(255), valid_time varchar(255)", quiet=True)
		grass.run_command('v.db.update', map='TMP_NetProcess_'+str(objpt), column='source', value='RACC_FORCITY'+str(objpt), quiet=True)
		grass.run_command('v.db.update', map='TMP_NetProcess_'+str(objpt), column='id', value='RACC_OK_'+ typeracc, quiet=True)
		
		''' grass.run_command('v.clean', flags='c', input='TP_Raccord_NetProcess_Finale', output='TP_Raccord_NetProcess_Finale_clean', tool='break,snap,chbridge,rmsa', threshold='0,0.5', quiet=True)	# grass.run_command('v.clean', flags='c', input='Raccord_prune2', output='Raccord_final10', tool='break,snap,chbridge,rmsa', threshold='0,0.5', quiet=True)
		'''

		grass.run_command('v.patch', flags='ea', input='TMP_NetProcess_'+str(objpt), output='TP_Raccord_NetProcess_Finale', quiet=True, overwrite=True)
		
		infoPB = grass.vector_info_topo('TMP_NetProcess_'+str(objpt))
		if infoPB['lines'] == 0:
			if typeracc == 'RCU':
				listerrorRCU.append(str(objpt))
			elif typeracc == 'ROUTES':
				listerrorRTE.append(str(objpt))
		grass.message("problemes : {0}".format(listerrorRCU))
		grass.message("problemes : {0}".format(listerrorRTE))

		''' Nettoyage fichier temporaires '''	
		tempfile = ['TMP_*']
		clean(tempfile)
		
		objpt+=1
	
	''' traitement des chaufferies posant problemes'''
	grass.message("traitements des erreurs")
	op = ' OR '
	if listerrorRCU:
		errorRCU=[]
		for f in listerrorRCU:
			expr='NUM='+str(f) 
			errorRCU.append(expr + op)
		grass.run_command('v.extract', input='TMPPP_NetProcess', output='TMPPP_NetProcess_PB_RCU', where=''.join(errorRCU)[:-4], quiet=True)
		grass.read_command('v.distance', flags='p', _from='TMPPP_NetProcess_PB_RCU', from_type='point', to='TMPPP_rsx_RCU', to_type='line', upload='dist', output='TP_NetProcess_PB_CONNECT_RCU', quiet=True)
		grass.run_command('v.db.addtable', map='TP_NetProcess_PB_CONNECT_RCU', columns="source varchar(255), id varchar(255), valid_time varchar(255)", quiet=True)
		grass.run_command('v.db.update', map='TP_NetProcess_PB_CONNECT_RCU', column='source', value='RACC_FORCITY', quiet=True)
		grass.run_command('v.db.update', map='TP_NetProcess_PB_CONNECT_RCU', column='id', value='RACC_RCU_AREVOIR', quiet=True)

	if listerrorRTE:
		errorRTE=[]
		for f in listerrorRTE:
			expr='NUM='+str(f) 
			errorRTE.append(expr + op)
		grass.run_command('v.extract', input='TMPPP_NetProcess', output='TMPPP_NetProcess_PB_RTE', where=''.join(errorRTE)[:-4], quiet=True)
		grass.read_command('v.distance', flags='p', _from='TMPPP_NetProcess_PB_RTE', from_type='point', to='TMPPP_rsx_ROUTES', to_type='line', upload='dist', output='TP_NetProcess_PB_CONNECT_RTE', quiet=True)
		grass.run_command('v.db.addtable', map='TP_NetProcess_PB_CONNECT_RTE', columns="source varchar(255), id varchar(255), valid_time varchar(255)", quiet=True)
		grass.run_command('v.db.update', map='TP_NetProcess_PB_CONNECT_RTE', column='source', value='RACC_FORCITY', quiet=True)
		grass.run_command('v.db.update', map='TP_NetProcess_PB_CONNECT_RTE', column='id', value='RACC_RTE_AREVOIR', quiet=True)
	
	vectraccordPB = grass.parse_command('g.list', type="vect", pattern="TP_NetProcess_PB_CONNECT_*", quiet=True)
	grass.run_command('v.edit', tool='create', map='TP_Raccord_NetPB_Finale', quiet=True)
	grass.run_command('v.db.addtable', map='TP_Raccord_NetPB_Finale', columns="source varchar(255), id varchar(255), valid_time varchar(255)", quiet=True)
	for vect in vectraccordPB:
		grass.run_command('v.patch', flags='ea', input=vect, output='TP_Raccord_NetPB_Finale', quiet=True, overwrite=True)
	
	''' assemblage des raccords dans une seule couche '''
	vectraccordFIN = grass.parse_command('g.list', type="vect", pattern="TP_Raccord_Net*", quiet=True)
	grass.run_command('v.edit', tool='create', map='TP_Raccord_Finale', quiet=True)
	grass.run_command('v.db.addtable', map='TP_Raccord_Finale', columns="source varchar(255), id varchar(255), valid_time varchar(255)", quiet=True)
	for vect in vectraccordFIN:
		grass.run_command('v.patch', flags='ea', input=vect, output='TP_Raccord_Finale', quiet=True, overwrite=True)			
	
	''' posttraitement des raccords '''
	grass.run_command('v.net', input='TP_Raccord_Finale', points='TMPP_Chauff', output='TP_Raccord_Finale_connect', operation='connect', thresh='0.1', quiet=True)
	grass.run_command('v.clean', input='TP_Raccord_Finale_connect', output='TP_Raccord_Finale_connect_clean', tool='break,snap', thresh='0,0.1', quiet=True)
	grass.run_command('v.generalize', flags='c', input='TP_Raccord_Finale_connect_clean', output='TP_Raccord_Finale_connect_clean_gene', method='lang', threshold='1', quiet=True)
	grass.run_command('v.clean', input='TP_Raccord_Finale_connect_clean_gene', output='TP_Raccord_Finale_connect_clean_gene_clean', tool='break,snap,rmdangle', thresh='0,0.1', quiet=True)

	grass.run_command('v.db.dropcolumn', map='TP_Raccord_Finale_connect_clean_gene_clean', columns='cat_', quiet=True)
	grass.run_command('v.patch', flags='ea', input=('TMPPP_rsx_RCU','TMPPP_rsx_ROUTES'), output='TP_Reseau', quiet=True, overwrite=True)
	grass.run_command('v.patch', input=('TP_Raccord_Finale_connect_clean_gene_clean','TP_Reseau'), output='TP_Reseau_RCU_complet', quiet=True)
	
	grass.run_command('v.clean', input='TP_Reseau_RCU_complet', output='Reseau_raccord', type='point,line', tool='snap,break,rmdangle,rmdupl,rmsa', thresh='0.1', quiet=True)
	
	''' export postgis '''
	SCHEM='public'
	grass.run_command('v.out.ogr', input='Reseau_raccord', type='line', output='PG:dbname=' + BDDNAME + ' host=' + IP + ' port=5432' + ' user=' + LOGIN + ' password=' + MDP + ' sslmode=require' + ' schemas=' + SCHEM, output_layer='Reseau_raccord', format='PostgreSQL')
	
	return 0

if __name__ == "__main__":
    options, flags = grass.parser()
    sys.exit(main())

