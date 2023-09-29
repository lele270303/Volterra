import pandas as pd

def season_aggregator(path, season=18):
    ## Adjusting dtypes 
    df=pd.read_csv(path)
    df['Data']=pd.to_datetime(df['Data'])
    df['Ruolo']=pd.Categorical(df['Ruolo'])
    df['Squadra']=pd.Categorical(df['Squadra'])
    df['Avversario']=pd.Categorical(df['Avversario'])
    ##

    ## Adding Fantavoto column
    def fantavoto(series):
        fantavoto=3*series[0]-series[1]+3*series[2]-3*series[3]+3*series[4]-2*series[5]-0.5*series[6]-series[7]+series[8]+series[9]
        return fantavoto

    column=[]
    fvotes=df[['Gf','Gs','Rp','Rs','Rf','Au','Amm','Esp','Ass','Voto']].to_numpy()

    for x in range(fvotes.shape[0]):
        column.append(fantavoto(fvotes[x,:]))

    df['Fantavoto']=column
    ##

    ## Starting aggregation
    season_df=df[df['Stagione']==season]
    aggregated=season_df.groupby(['Cod.']).agg({'Stagione':'min', 'Cod.':'min','Nome':'min','Quota':'max','Ruolo':'last', 'Squadra':'last',
                                                            'Giornata':'count','Gf':'sum', 'Gs':'sum', 'Rp':'sum','Rs':'sum','Rf':'sum','Au':'sum','Amm':'sum',
                                                            'Esp':'sum','Ass':'sum','Voto':'mean','Fantavoto':'mean'})

    #Renaming columns
    aggregated.rename({'Ruolo':'Ruolo','Giornata':f'Presenze_stagione_{season}','Squadra':f'Squadra_stagione_{season}','Gf':f'Goal_stagione_{season}','Gs':f'Goal_subiti_{season}','Rp':f'Rigori_parati_{season}','Rf':f'Rigori_sbagliati_{season}'
                    ,'Rf':f'Rigori_segnati_{season}','Au':'Autoreti','Amm':f'Ammonizioni_stagione_{season}','Esp':f'Espulsioni_stagione_{season}','Ass':f'Assist_stagione_{season}',
                    'Voto':f'Media_base_{season}','Fantavoto':f'Fantamedia_{season}'
                    }, axis=1, inplace=True)

    def round_float(x):
        return round(x,2)

    aggregated[[f'Media_base_{season}',f'Fantamedia_{season}']]=aggregated[[f'Media_base_{season}',f'Fantamedia_{season}']].apply(round_float)
    aggregated[f'Goal_stagione_{season}']=aggregated[f'Goal_stagione_{season}']+aggregated[f'Rigori_segnati_{season}']
    aggregated[f'Goal_subiti_{season}']=aggregated[f'Goal_subiti_{season}']+aggregated[f'Rigori_segnati_{season}']

    ## Rankings
    class Team:
        def __init__(self, name) -> None:
            self.name=name
            self.points=0
            self.goals=0
            self.against=0
            self.gd=0

    class League:
        def __init__(self, df, team_list) -> None:
            self.df=df
            self.teams=team_list
            self.d={team.name:team for team in self.teams}
            self.all_ranks=[]
            self.loaded=False

        def load(self):

            for matchday in range(1,39):
                mdf=self.df[self.df['Matchday']==matchday]
                #Adding for each matchday information
                for index, row in mdf.iterrows():
                    home_team=self.d[row['HomeTeam']]
                    away_team=self.d[row['AwayTeam']]
                    home_goals=row['FTHG']
                    away_goals=row['FTAG']

                    home_team.goals+=home_goals
                    away_team.goals+=away_goals

                    home_team.against+=away_goals
                    away_team.against+=home_goals

                    home_team.gd=home_team.goals-home_team.against
                    away_team.gd=away_team.goals-away_team.against

                    if home_goals > away_goals:

                        home_team.points+=3
                    elif home_goals < away_goals:
                        away_team.points+=3
                    else:
                        home_team.points+=1
                        away_team.points+=1

            
                ranking=pd.DataFrame(columns=['Matchday','Team','Points','GD'])
        
                for team in self.teams:
                    attach=pd.DataFrame({ 'Matchday': [matchday], 'Team': [team.name], 'Points': [team.points], 'GD':[team.gd]})
                    ranking=pd.concat( [ ranking, attach ] , ignore_index=True)
                
                ranking.sort_values(['Points','GD'], ascending=False ,inplace=True)
                ranking['Rank']=[x for x in list(range(1,21))]  
        
                self.all_ranks.append(ranking) 
            
            self.loaded=True


        def final(self):
            if self.loaded:
                print(self.all_ranks[-1]) 
            else:
                self.load()  
                print(self.all_ranks[-1])      
    ##
    ## Other columns - perc/team

    ##goals
    df=pd.read_csv('/Users/emanuelesebastianelli/Desktop/Fantaset/DatasetCreation/leaguesFAKE/18.csv')

    team_list=[Team(name) for name in df['HomeTeam'].unique().tolist()]

    lega=League(df=df, team_list=team_list)
    lega.load()
    #goal totali
    goal_totali={team.name:team.goals for team in team_list}
    #goal perc
    perc_goal_team=[]
    for index, row in aggregated.iterrows():
        perc_goal_team.append(row[f'Goal_stagione_{season}'] / goal_totali.get(row[f'Squadra_stagione_{season}'], 50))


    aggregated[f'Perc_goal_team_{season}']=perc_goal_team
    #goal a partita
    aggregated[f'Media_goal_giocate_{season}']=round(aggregated[f'Goal_stagione_{season}']/aggregated[f'Presenze_stagione_{season}'],3)

    #goal in quante partite
    marcature=season_df[(season_df['Gf']>0 )| (season_df['Rf']>0)].groupby('Cod.')['Squadra'].agg('count').to_dict()
    aggregated[f'Numero_partite_marcatura_{season}']=[marcature.get(row['Cod.'],0) for index, row in aggregated.iterrows()]

    ##assists
    #assist/squadra
    perc_ass_team=[]
    assist_totali=season_df.groupby('Squadra')['Ass'].agg(sum).to_dict()
    for index, row in aggregated.iterrows():
        perc_ass_team.append(row[f'Assist_stagione_{season}']/assist_totali[row[f'Squadra_stagione_{season}']]*100)

    aggregated[f'Perc_assist_team_{season}']=perc_ass_team


    #assist/giocate
    aggregated[f'Media_assist_giocate_{season}']=round(aggregated[f'Assist_stagione_{season}']/aggregated[f'Presenze_stagione_{season}'],3)

    #goal in quante partite
    assist_paritite=season_df[season_df['Ass']>0].groupby('Cod.')['Squadra'].agg('count').to_dict()
    aggregated[f'Numero_partite_assist_{season}']=[assist_paritite.get(row['Cod.'],0) for index, row in aggregated.iterrows()]
    ## miss matches
    #total
    def mdays_out(series):
        series=set(series)
        all=set(range(1,38))
        return len(all.symmetric_difference(series))


    days_out=season_df.groupby('Cod.')['Giornata'].agg(mdays_out).to_dict()
    aggregated[f'Parite_saltate_{season}']=[days_out[row['Cod.']] for index, row in aggregated.iterrows()]
    #max
    def mdays_out_max(series):
        series=set(series)
        all=set(range(1,38))
        left=sorted(list(all.symmetric_difference(series)), reverse=False)
        if len(left)>=1:
            max=0
            current=0
            start=left[0]-1
            for x in left:
                if start+1==x:
                    current+=1
                if current>max:
                    max=current
                start=x
            return max
        else:
            return 0

    max_days_out=season_df.groupby('Cod.')['Giornata'].agg(mdays_out_max).to_dict()
    aggregated[f'Parite_saltate_max_{season}']=[max_days_out[row['Cod.']] for index, row in aggregated.iterrows()]

    ##ammonizioni
    #ammonizioni a partita
    aggregated[f'Ammonizioni_partita_{season}']=[ round(row[f'Ammonizioni_stagione_{season}']/row[f'Presenze_stagione_{season}'],2) for index, row in aggregated.iterrows()]
    #espulsioni a partita
    aggregated[f'Espulsioni_partita_{season}']=[ round(row[f'Espulsioni_stagione_{season}']/row[f'Presenze_stagione_{season}'],2) for index, row in aggregated.iterrows()]

    #sufficenze_partita
    def suff(series):
        for n in series:
            n_suf=0
            if n>=6:
                n_suf+=1
        return n_suf

    perc_suff_d=season_df.groupby('Cod.')['Fantavoto'].agg(suff).to_dict()

    aggregated[f'Sufficienze_partite_{season}']=[perc_suff_d[row['Cod.']]/row[f'Presenze_stagione_{season}']*100 for index, row in aggregated.iterrows()]

        
    #Gravi insufficienze
    def insuff(series):
        for n in series:
            n_insuf=0
            if n<=4:
                n_insuf+=1
        return n_insuf

    perc_gravi_d=season_df.groupby('Cod.')['Fantavoto'].agg(suff).to_dict()
    aggregated[f'Insuff_gravi_partite_{season}']=[perc_gravi_d[row['Cod.']]/row[f'Presenze_stagione_{season}']*100 for index, row in aggregated.iterrows()]

    #Perc pres squadra reparto



    #Prima linea >meta partite

    aggregated[f'Over_metà_stagione_{season}']=[True if row[f'Presenze_stagione_{season}']>19 else False for index, row in aggregated.iterrows()]

    #
    aggregated[f'Over_3/4_stagione_{season}']=[True if row[f'Presenze_stagione_{season}']>28 else False for index, row in aggregated.iterrows() ]

    return aggregated

def multi_aggregator(path, current_season=23):
    fanta_df=pd.read_csv(path)
    fanta_df=fanta_df[fanta_df['Stagione']<current_season]
    agg_list=[]
    for x in range(15,current_season):
        agg_list.append(season_aggregator(path,x))
    current_season_df=agg_list[-1]
    season_player_list=current_season_df['Cod.'].unique().tolist()
    ## Totals 
    #Total Presenze 
    total_games=fanta_df.groupby('Cod.')['Squadra'].agg('count').to_dict()
    current_season_df['Presenze_Totali']=current_season_df['Cod.'].map(total_games)

    #Total goals
    total_goals_dict=fanta_df.groupby('Cod.')[['Gf','Rf']].agg(sum)
    total_goals_dict['Total']= total_goals_dict['Gf']+total_goals_dict['Rf']
    total_goals_dict=total_goals_dict.iloc[:,-1:].to_dict()
    total_goals_dict=total_goals_dict['Total']
    current_season_df['Goal_Totali']=current_season_df['Cod.'].map(total_goals_dict)

    #Total penalties scored
    total_pen=fanta_df.groupby('Cod.')['Rs'].agg('sum').to_dict()
    current_season_df['Rigori_segnati_Totali']=current_season_df['Cod.'].map(total_pen)

    #Total goals subiti
    total_subiti=fanta_df.groupby('Cod.')['Gs'].agg('sum').to_dict()
    current_season_df['Goal_Subiti_Totali']=current_season_df['Cod.'].map(total_subiti)

    #Total rigori parati
    total_pen_saved=fanta_df.groupby('Cod.')['Rp'].agg('sum').to_dict()
    current_season_df['Rigori_segnati_Totali']=current_season_df['Cod.'].map(total_pen_saved)

    #Total_assist
    total_assists_dict=fanta_df.groupby('Cod.')['Ass'].agg(sum).to_dict()
    current_season_df['Assist_Totali']=current_season_df['Cod.'].map(total_assists_dict)
    

    #Number of teams played in
    n_of_teams=fanta_df.groupby('Cod.')['Squadra'].agg('nunique').to_dict()
    current_season_df['Squadre_Diverse']=current_season_df['Cod.'].map(n_of_teams)

    #Number of seasons played 
    n_of_seasons=fanta_df.groupby('Cod.')['Stagione'].agg('nunique').to_dict()
    current_season_df['Stagioni_Giocate']=current_season_df['Cod.'].map(n_of_seasons)

    #Total yellow cards

    yellow=fanta_df.groupby('Cod.')['Amm'].agg('sum').to_dict()
    current_season_df['Ammonizioni_Totali']=current_season_df['Cod.'].map(yellow)

    #Total Red cards

    red=fanta_df.groupby('Cod.')['Esp'].agg('sum').to_dict()
    current_season_df['Espulsioni_Totali']=current_season_df['Cod.'].map(red)

    ## Averages and others
    #Media voto 
    mm=fanta_df.groupby('Cod.')['Voto'].agg('mean').to_dict()
    current_season_df['Media_base_Totale']=current_season_df['Cod.'].map(mm)
    current_season_df['Media_base_Totale']=round(current_season_df['Media_base_Totale'],3)
    #Fantamedia
    fmm=fanta_df.groupby('Cod.')['Fantavoto'].agg('mean').to_dict()
    current_season_df['Fantamedia_Totale']=current_season_df['Cod.'].map(fmm)
    current_season_df['Fantamedia_Totale']=round(current_season_df['Fantamedia_Totale'],3)

    #Avg Goal Presenza / Season
    current_season_df['Goal_Presenza']=round(current_season_df['Goal_Totali']/current_season_df['Presenze_Totali'],3)
    current_season_df['Goal_Stagione']=round(current_season_df['Goal_Totali']/current_season_df['Stagioni_Giocate'],3)

    #Avg Ass Presenza / Season
    current_season_df['Assist_Presenza']=round(current_season_df['Assist_Totali']/current_season_df['Presenze_Totali'],3)
    current_season_df['Assist_Stagione']=round(current_season_df['Assist_Totali']/current_season_df['Stagioni_Giocate'],3)

    current_season_df['G/A_Partita']=current_season_df['Goal_Presenza']+current_season_df['Assist_Presenza']
    current_season_df['G/A_Stagione']=current_season_df['Goal_Stagione']+current_season_df['Assist_Stagione']

    #Goal subiti
    current_season_df['Goal_Subiti_Partita']=round(current_season_df['Goal_Subiti_Totali']/current_season_df['Presenze_Totali'],3)
    current_season_df['Goal_Subiti_Stagione']=round(current_season_df['Goal_Subiti_Totali']/current_season_df['Stagioni_Giocate'],3)

    #Quota media 
    Average_quota_dict=fanta_df.groupby('Cod.')['Quota'].agg('mean').to_dict()
    current_season_df['Quota_media']=current_season_df['Cod.'].map(Average_quota_dict)
    current_season_df['Quota_media']=round(current_season_df['Quota_media'],3)

    #Cambio percentuale di quota
    def min_max_change(series):
        if len(series)==0:
            return 0
        else:
            beginning=series[0]
            end=series[-1:]
            return (end-beginning)*100/beginning
        
    #Change_in_season=fanta_df.groupby('Cod.')['Quota'].agg(min_max_change).to_dict()
    #current_season_df['Cambio_quota_totale']=current_season_df['Cod.'].map(Change_in_season)

    def mean_change(series):
        if len(series)==0:
            return 0
        else:
            l=[]
            for x in range(len(series)):
                if x==len(series):
                    break
                else:
                    l.append((series[x+1]-series[x])*100/series[x])
            return sum(l)/len(l)
    #mean_change_quota=fanta_df.groupby('Cod.')['Quota'].agg(mean_change).to_dict()
    #current_season_df['Cambio_quota_medio']=current_season_df['Cod.'].map(mean_change_quota)
    

    return current_season_df

res=multi_aggregator('/Users/emanuelesebastianelli/Desktop/Fantaset/Fanta/Fantacalcio.csv',23)
print(res)




