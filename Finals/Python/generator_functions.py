"""
Created on Wed Mar  1 17:34:31 2023

@author: David Hurtgen

This program contains all the functions necessary for records generation

6-27-2023 - code rewritten as a class with all generator functions contained therein
"""

import random

class GeneratorFunctions():
    """ 
    instantiates an object for creating various tables
    """

    def __init__(self):
        # band name nouns and adjectives
        self._noun_list = ['Anemone','Bandwagon','Concoction','Doldrums','Epiphany',\
                     'Flummery','Gambit','Halfwit','Interloper','Jabberwocky',\
                         'Kibosh','Loophole','Misanthrope','Nudnik','Oxymoron',\
                             'Pedagogue','Ruckus','Sobriquet','Typhoon','Windbag']
            
        self._adjective_list = ['Apoplectic','Blubbering','Cantankerous','Diaphanous','Extraterrestrial',\
                          'Flummoxed','Gobsmacked','Harebrained','Indomitable','Lugubrious',\
                              'Mellifluous','Orotund','Pulchritudinous','Quiddling','Ragtag',\
                                  'Slipshod','Toothsome','Vainglorious','Wuthering','Zigzag']
        
        # mics by model
        self._vox_mics = ['OM7','M 88 TG','d:facto','SR314','767a N/D','PR35','KMS 105','e935','Beta 58A','SM58','M80']
                        
        self._inst_mics = ['i5','M 69 TG','M 88 TG','M 160','M 201 TG','SR25','RE16','RE20','PR30','R-121','e906','MD 421','MD 441 U','Beta 57A',\
                     'SM7B','SM57']
            
        self._kick_mics = ['D6','PL35','RE20','e902','Beta 52A']
        
        self._snare_mics = ['i5','M 201 TG','MD 441 U','SM57']
          
        self._tom_mics = ['D2','e904','MD 421','Beta 98AD/C']
            
        self._area_mics = ['C414 XLII','AT4040','635a','KM 184','CMC64','SM81']
        
        # roles
        self._roles = ['Backup Vocalist','Guitarist','Horn Player','Keyboardist','Percussionist','String Player']
        
        # venue size
        self._venue_sizes = ['Small','Medium','Large']
        
        # results
        self._results = ['Poor','Mediocre','Average','Good','Excellent']
        
        # band styles
        self._styles = ['Folk','Hip-Hop','Jazz','Pop','Rock']
        
        # mic list for dimMics
        self._mic_records = [[0,'AKG','C414 XLII','Condenser'], [1,'Audio-Technica','AT4040','Condenser'], [2,'Audix','D2','Dynamic'], \
                            [3,'Audix','D6','Dynamic'], [4,'Audix','i5','Dynamic'], [5,'Audix','OM7','Dynamic'], \
                                [6,'Beyerdynamic','M 69 TG','Dynamic'], [7,'Beyerdynamic','M 88 TG','Dynamic'], [8,'Beyerdynamic','M 160','Ribbon'], \
                                    [9,'Beyerdynamic','M 201 TG','Dynamic'], [10,'DPA','d:facto','Condenser'], [11,'Earthworks','SR25','Condenser'], \
                                        [12,'Earthworks','SR314','Condenser'], [13,'Electro-Voice','635a','Dynamic'], \
                                            [14,'Electro-Voice','767a N/D','Dynamic'], [15,'Electro-Voice','PL35','Dynamic'], \
                                                [16,'Electro-Voice','RE16','Dynamic'], [17,'Electro-Voice','RE20','Dynamic'], \
                                                    [18,'Heil','PR30','Dynamic'], [19,'Heil','PR35','Dynamic'], [20,'Neumann','KM 184','Condenser'], \
                                                        [21,'Neumann','KMS 105','Condenser'], [22,'Royer','R-121','Ribbon'], \
                                                            [23,'Schoeps','CMC64','Condenser'], [24,'Sennheiser','e902','Dynamic'], \
                                                                [25,'Sennheiser','e904','Dynamic'], [26,'Sennheiser','e906','Dynamic'], \
                                                                    [27,'Sennheiser','e935','Dynamic'], [28,'Sennheiser','MD 421','Dynamic'], \
                                                                        [29,'Sennheiser','MD 441 U','Dynamic'], [30,'Shure','Beta 52A','Dynamic'], \
                                                                            [31,'Shure','Beta 57A','Dynamic'], [32,'Shure','Beta 58A','Dynamic'], \
                                                                                [33,'Shure','Beta 98AD/C','Condenser'], [34,'Shure','SM7B','Dynamic'], \
                                                                                    [35,'Shure','SM57','Dynamic'], [36,'Shure','SM58','Dynamic'], \
                                                                                        [37,'Shure','SM81','Condenser'], [38,'Telefunken','M80','Dynamic']]
            
        # source list for dimSource
        self._source_records = [[0,'Backup Vocal'], [1,'Bass'], [2,'Drums, Kick'], [3,'Drums, Snare'], [4,'Drums, Hi-hat'], [5,'Drums, Toms'], \
                               [6,'Drums, Overhead'], [7,'Guitar'], [8,'Horn'], [9,'Keyboards'], [10,'Lead Vocal'], [11,'Percussion'], [12,'Strings']]

    # generator functions
    def generate_venue_name(self):
        venue_name = "The" + " " + random.choice(self._adjective_list) + " " + random.choice(self._noun_list)
        return venue_name
    
    def generate_venue_size(self):
        venue_size = random.choice(self._venue_sizes)
        return venue_size
    
    def generate_fact_result(self):       
        fact_result = random.choice(self._results)
        return fact_result
        
    def generate_band_name(self):
        band_name = random.choice(self._adjective_list) + " " + random.choice(self._noun_list)
        return band_name
        
    def generate_style(self):
        style = random.choice(self._styles)
        return style
        
    def generate_role(self):
        role = random.choice(self._roles)
        return role
       
    def generate_vox_mic(self):
        vox_mic = random.choice(self._vox_mics)
        return vox_mic
    
    def generate_inst_mic(self):
        inst_mic = random.choice(self._inst_mics)
        return inst_mic
    
    def generate_kick_mic(self):
        kick_mic = random.choice(self._kick_mics)
        return kick_mic
    
    def generate_snare_mic(self):
        snare_mic = random.choice(self._snare_mics)
        return snare_mic
    
    def generate_tom_mic(self):
        tom_mic = random.choice(self._tom_mics)
        return tom_mic
    
    def generate_area_mic(self):
        area_mic = random.choice(self._area_mics)
        return area_mic
    
    def generate_reverberance_length(self, venue_size):
        """
        function to generate reverberance length in seconds
        :param venue_size: one of three used to calculate reverberance
        """
        if venue_size=='Small':
            mu=1.0
            sigma=0.2
            reverberance_length = round(random.normalvariate(mu, sigma), 2)
            return reverberance_length
        elif venue_size=='Medium':
            mu=1.5
            sigma=0.3
            reverberance_length = round(random.normalvariate(mu, sigma), 2)
            return reverberance_length
        else:
            mu=2.0
            sigma=0.5
            reverberance_length = round(random.normalvariate(mu, sigma), 2)
            return reverberance_length
    
    # These are funcions necessary for the main alogrithm
    def get_source_id(self, role):
        """
        function to get source_id, excludes Drummers
        :param role: one of eight used to determine source_id, which is 
                     then used as a parameter for choose_mic_generator()
        """
        if role=='Backup Vocalist':
            return 0
        elif role=='Bassist':
            return 1
        elif role=='Guitarist':
            return 7
        elif role=='Horn Player':
            return 8
        elif role=='Keyboardist':
            return 9
        elif role=='Lead Vocalist':
            return 10
        elif role=='Percussionist':
            return 11
        else:
            return 12
    
    def get_sid_drummers(self, drum):
        """
        function to get source_id for Drummers only
        :param drum: one of five used to determine source_id, which is 
                     then used as a parameter for choose_mic_generator()
        """
        if drum=='Kick':
            return 2
        elif drum=='Snare':
            return 3
        elif drum=='Hi-hat':
            return 4
        elif drum=='Toms':
            return 5
        else:
            return 6
        
    def choose_mic_generator(self, source_id):
        """
        a function to generate mics
        :param source_id: used to determine what category of mic
                          to generate
        """
        if source_id==0 or source_id==10:
            result=self.generate_vox_mic()
            return result
        elif source_id in [1,7,8]:
            result=self.generate_inst_mic()
            return result
        elif source_id==2:
            result=self.generate_kick_mic()
            return result
        elif source_id==3:
            result=self.generate_snare_mic()
            return result
        elif source_id==5:
            result=self.generate_tom_mic()
            return result
        else:
            result=self.generate_area_mic()
            return result
    
    def get_mic_id(self, mic_used):
        for mic in self._mic_records:
            if mic[2]==mic_used:
                return mic[0]
                break
    
    def result_to_numeric(self, x):
        """
        function to generate a 'result_numeric' column 
        :param x: a result category to convert to a numeric value
        """
        if x == 'Poor':
            result = 1.0
        elif x == 'Mediocre':
            result = 2.0
        elif x == 'Average':
            result = 3.0
        elif x == 'Good':
            result = 4.0
        else:
            result = 5.0
        return result
    
    def add_micused_fact_drums(self, source, mic_used_records, member_records, fact_records, band_id):
        """
        function for generating mic_used and fact records for drums only
        :param source: the source element as a string
        :param mic_used_records: used to determine appropriate mic_used_id
        :param member_records: used to determine appropriate member_id
        :param fact_records: used to determine appropriate fact_id
        :param band_id: appropriate band_id
        """
        # add mic used
        source_id=self.get_sid_drummers(source)
        mic_name=self.choose_mic_generator(source_id)
        mic_id=self.get_mic_id(mic_name)
        mic_used_records.append([mic_used_records[-1][0]+1, member_records[-1][0], source_id, mic_name, mic_id])
        
        # add fact
        venue_id=random.randint(0, 99)
        result=self.generate_fact_result()
        fact_records.append([fact_records[-1][0]+1, mic_id, band_id, venue_id, source_id, result])    

    
# End of program