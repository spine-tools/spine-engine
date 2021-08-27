######################################################################################################################
# Copyright (C) 2017-2021 Spine project consortium
# This file is part of Spine Engine.
# Spine Engine is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Contains a helper class for converting received event/data information to JSON-based format
:authors: P. Pääkkönen (VTT)
:date:   27.08.2021
"""

class EventDataConverter:


    


    """
    Converts events+data
    Args:
        eventData(a list of tuples containing events and data)
    Return:
        JSON string
    """


    @staticmethod
    def convert(eventData):
        itemCount=len(eventData)
        i=0
        retStr="{\n"
        retStr+="    \"items\": [\n"
        for ed in eventData:
            #print(ed)
            retStr+="    {\n        \"event_type\": \""+ed[0]+"\",\n"
            if (i+1) < itemCount:
                retStr+="        \"data\": \""+str(ed[1])+"\"\n    },\n"
                #print("orig dict:")
                #print(ed[1])
                #print("modified to str:")
                #print(str(ed[1]))
            else:
                retStr+="        \"data\": \""+str(ed[1])+"\"\n    }\n"
            i+=1
        retStr+="    ]\n"
        retStr+="}\n"
        return retStr
