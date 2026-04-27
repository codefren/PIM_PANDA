import re
import time,os
from datetime import datetime
from openai import OpenAI
import openai

api_key = os.getenv("OPENAI_API_KEY")
class OpenAI_FashionTraductor:

    def __init__(self,api_key,days_to_delete_logs=7):
        self.api_key = api_key
        self.assistant_id = None
        self.client = OpenAI(api_key=api_key)
        self.assistant = self.client.beta.assistants.retrieve(assistant_id=self.assistant_id)
        self.threads = [self.client.beta.threads.retrieve(thread_id="thread_YlIR1fqI2xucBD2riUQCffA6")] if self.client.beta.threads.retrieve(thread_id="thread_YlIR1fqI2xucBD2riUQCffA6") else []
        self._delete_old_logs(days_to_delete_logs)

    def _write_log(self,msg,status:str='NONE'):
        if not os.path.exists('logs'):
            os.makedirs('logs')
        if not os.path.exists('logs/logs_openai'):
            os.makedirs('logs/logs_openai')
        with open(f'logs/logs_openai/log_openai_{datetime.now().strftime("%d_%m_%Y")}.log','a') as file:
            file.write(f"{datetime.now().strftime('%H:%M:%S')}      {status}    {msg}\n")

    def _delete_old_logs(self,days=7):
        logs_path = 'logs/logs_openai'  # Ruta al directorio de los logs
        if os.path.exists(logs_path):
            for file_name in os.listdir(logs_path):
                full_file_path = os.path.join(logs_path, file_name)  # Construye la ruta completa del archivo
                if file_name.endswith('.log'):
                    file_creation_time = datetime.fromtimestamp(os.path.getctime(full_file_path))
                    if (datetime.now() - file_creation_time).days > days:
                        os.remove(full_file_path)


    def call_api_with_retry(self, api_call, _k=1, *args, **kwargs):
        while True:
            try:
                #time.sleep(0.3)
                #self._write_log("Calling API",str(api_call))
                response = api_call(*args, **kwargs)
                try:
                    if response.status_code == 429:
                        self._write_log('waiting 2 secs, rate exceeded')
                        time.sleep(2*_k)  # Wait for 2 seconds before retrying
                        _k += 1
                    else:
                        return response
                except Exception as e:
                    return response
            except openai.BadRequestError as e:
                raise e
    def get_main_thread(self):
        return self.threads[0] if self.threads else self.create_thread()

    def __create_assistant(self,model="gpt-4o",name='Fashion Traductor',
                         instructions="You are a specialist in the fashion industry and know many languages "
                                      "and all the technicalities of this sector. Your role is to translate "
                                      "text in the context of fashion in a marketing and very human manner.",
                         tools=[]):
        self.assistant = self.call_api_with_retry(self.client.beta.assistants.create,name= name,
            instructions=instructions,
            tools=tools,
            model=model)

    def create_thread(self):
        if self.assistant:
            thread = self.call_api_with_retry(self.client.beta.threads.create)
            self.threads.append(thread)
            return True

        self.__create_assistant()
        thread = self.call_api_with_retry(self.client.beta.threads.create)
        self.threads.append(thread)
        return thread

    def is_valid_thread(self,thread_id):
        if self.assistant and self.threads:
            return thread_id in [thread.id for thread in self.threads]
        return False

    def create_message(self,msg,thread_id,_k=1):
        if self.assistant and self.threads:

            if not self.is_valid_thread(thread_id):
                raise Exception('Thread_id not valid')
            try:
                return self.call_api_with_retry(self.client.beta.threads.messages.create,thread_id=thread_id,
                    role="user",
                    content=msg)
            except openai.BadRequestError as e:
                error_message = str(e)
                self._write_log('Catching error message: ', error_message)
                # Comprobar si el error se debe a una run activa
                if "run" in error_message and "is active" in error_message:
                    # Buscar el ID de la run utilizando una expresión regular
                    match = re.search(r'run_(\w+)', error_message)
                    if match:
                        run_id = match.group(0)
                        self._write_log(run_id)
                        if self.retrieve_run(thread_id, run_id).status != 'cancelling':
                            self.cancel_run(thread_id, run_id)
                        else:
                            self._write_log('Run being cancelled, in create_message()', 'INFO')
                            self.threads = []
                            self.create_thread()
                            return self.create_message(msg, self.get_main_thread().id,_k=_k)

                        time.sleep(2*_k)
                        return self.create_message(msg, thread_id,_k=_k+1)
                    else:
                        self._write_log('No run ID found in error message: ', error_message)
                        self.threads=[]
                        self.create_thread()
                        return self.create_message(msg, self.get_main_thread().id)
                else:
                    raise e

        raise Exception('Assistant or threads not created')


    def run_thread(self,thread_id,instructions=None):
        if self.assistant and self.threads:

            if not self.is_valid_thread(thread_id):
                if not self.create_thread():
                    raise Exception('Thread_id not valid')
                self.threads.reverse()
                thread_id = self.get_main_thread().id


            return self.call_api_with_retry(self.client.beta.threads.runs.create,thread_id=thread_id,
                assistant_id = self.assistant.id,
                instructions=instructions)

        raise Exception('Assistant or threads not created')

    def check_run_state(self,thread_id,run_id):
        if self.assistant and self.threads:

            if not self.is_valid_thread(thread_id):
                raise Exception('Thread_id not valid')

            r = self.call_api_with_retry(self.client.beta.threads.runs.retrieve,thread_id=thread_id,
                run_id=run_id)
            return r
        raise Exception('Assistant or threads not created')

    def cancel_run(self,thread_id,run_id):
        if self.assistant and self.threads:

            if not self.is_valid_thread(thread_id):
                raise Exception('Thread_id not valid')

            return self.call_api_with_retry(self.client.beta.threads.runs.cancel,thread_id=thread_id,
                run_id=run_id)
        raise Exception('Assistant or threads not created')

    def retrieve_run(self,thread_id,run_id):
        if self.assistant and self.threads:

            if not self.is_valid_thread(thread_id):
                raise Exception('Thread_id not valid')

            return self.call_api_with_retry(self.client.beta.threads.runs.retrieve,thread_id=thread_id,
                run_id=run_id)
        raise Exception('Assistant or threads not created')

    def display_messages(self,thread_id):
        if self.assistant and self.threads:

            if not self.is_valid_thread(thread_id):
                raise Exception('Thread_id not valid')

            return self.call_api_with_retry(self.client.beta.threads.messages.list,thread_id=thread_id)

        raise Exception('Assistant or threads not created')


    def translate(self,l_ori,l_dest,msg,thread_id,_k=1):
        if self.assistant and self.threads:
            if not self.is_valid_thread(thread_id):
                self.create_thread()
                self.threads.reverse()
                thread_id = self.get_main_thread().id

            if l_ori == l_dest:
                return msg
            self.create_message("Give me ONLY the text translated, Translate from '" + l_ori + "' to '" + l_dest + "' this text: " + msg.lower(), thread_id)
            run = self.run_thread(thread_id)
            while self.check_run_state(thread_id,run.id).status != 'completed':
                state = self.check_run_state(thread_id,run.id)
                if state.status == 'failed':
                    self._write_log(f'Run {self.retrieve_run(thread_id,run.id)} failed, retrying...')
                    try: self._write_log('Canceling run');self.cancel_run(thread_id,run.id)
                    except: pass
                    time.sleep(2*_k)
                    _k +=1
                    try:
                        self._write_log('Canceling thread')
                        self.create_thread()
                        self.threads.reverse()
                        thread_id = self.get_main_thread().id
                        self._write_log(f'New thread created: {thread_id}')
                    except:
                        pass
                    run = self.run_thread(thread_id)
                    state = self.check_run_state(thread_id,run.id)
                time.sleep(1)
                if state.status == 'cancelled':
                    try:
                        self.create_thread()
                        self.threads.reverse()
                        thread_id = self.get_main_thread().id
                        self._write_log(f'New thread created: {thread_id}')
                    except:
                        pass
                    run = self.run_thread(thread_id)
                    state = self.check_run_state(thread_id, run.id)

            messages = self.display_messages(thread_id)
            for message in messages:
                if message.role == 'assistant':
                    self._write_log(f'Traduction generated, origen: {msg}, dest: {message.content[0].text.value}')
                    res = message.content[0].text.value
                    if msg.isupper(): res = res.upper()
                    if msg.istitle(): res = res.title()
                    return res

        raise Exception('Assistant or threads not created')

    def detect_language(self,msg,thread_id):
        if self.assistant and self.threads:
            if not self.is_valid_thread(thread_id):
                raise Exception('Thread_id not valid')

            self.create_message("Give me only the format ISO 639-1 of the language of this text: " + msg,thread_id)
            run = self.run_thread(thread_id)
            while self.check_run_state(thread_id,run.id).status != 'completed':
                state = self.check_run_state(thread_id,run.id)
                if state.status == 'failed':
                    self._write_log(f'Run {run.id} failed, retrying...')
                    self._write_log(f"state")
                    self.cancel_run(thread_id, run.id)
                    time.sleep(5)
                    run = self.run_thread(thread_id)
                    state = self.check_run_state(thread_id, run.id)
                if state.status == 'cancelled':
                    raise Exception('Run cancelled',state)

            messages = self.display_messages(thread_id)
            for message in messages:
                if message.role == 'assistant':
                    return str(message.content[0].text.value).upper()

        raise Exception('Assistant or threads not created')



class Traductor(OpenAI_FashionTraductor):
    def __init__(self,db,api_key=key):
        super().__init__(api_key)
        self.thread_id = self.get_main_thread().id
        self._db = db


    def save_translation(self,l_ori,l_dest,text_ori,text_dest):
        if self.get_translation(l_ori, l_dest, text_ori):
            return False
        if text_dest == '' or text_dest is None:
            return False
        q = "insert into tbdtraduccionesOpenAI (fldIdiomaorigen,fldidiomadestino,fldtextoorigen,fldtextodestino) values (?,?,?,?)"
        self._db.execute(q,[l_ori,l_dest,text_ori[:500],text_dest[:500]])
        self._db.commit()
        return True

    def save_batch_translations_overwrite(self,data:list[tuple[str,str,str,str]]):
        if not data:
            return True
        self._write_log(f"Saving batch translations of len {len(data)}")
        self._db.execute_many("insert into tbdtraduccionesOpenAI (fldIdiomaorigen,fldidiomadestino,fldtextoorigen,fldtextodestino) values (?,?,?,?)",data)
        self._db.commit()
        self._write_log("Batch translations saved")
        return True

    def overwrite_translation(self,l_ori,l_dest,text_ori,text_dest):
        if self.save_translation(l_ori,l_dest,text_ori,text_dest) is not False:
            return True
        q = "update tbdtraduccionesOpenAI set fldtextodestino = ? where fldidiomaorigen = ? and fldidiomadestino = ? and fldtextoorigen = ?"
        self._db.execute(q, [text_dest, l_ori, l_dest, text_ori])
        self._db.commit()
        return True

    def delete_translation(self,l_ori,l_dest,text_ori,text_dest):
        q = "delete from tbdtraduccionesopenai where fldidiomaorigen = ? and fldidiomadestino = ? and fldtextoorigen = ? and fldtextodestino = ?"
        self._db.execute(q, [l_ori, l_dest, text_ori,text_dest])
        self._db.commit()
        return True

    def get_translation(self,l_ori,l_dest,text_ori):
        q = "select fldtextodestino from tbdtraduccionesOpenAI where fldidiomaorigen = ? and fldidiomadestino = ? and fldtextoorigen = ?"
        self._db.execute(q,[l_ori,l_dest,text_ori])
        res = self._db.fetchall()
        for row in res:
            if row and row[0] and row[0] != '':
                return row[0]
        return None

    def get_batch_translations(self,data:list[dict[str:str]]):
        idiomas_origen = set(row['l_ori'] for row in data)
        idiomas_destino = set(row['l_dest'] for row in data)
        textos_origen = set(row['text_ori'] for row in data)

        # Realizar la consulta a la base de datos una sola vez
        q = f"""SELECT fldidiomaorigen, fldidiomadestino, fldtextoorigen, fldtextodestino
                   FROM tbdtraduccionesOpenAI
                   WHERE fldidiomaorigen IN ({str('?,'*len(idiomas_origen))[:-1]}) AND fldidiomadestino IN ({str('?,'*len(idiomas_destino))[:-1]}) AND fldtextoorigen IN ({str('?,'*len(textos_origen))[:-1]})"""
        # Ajusta la consulta anterior a la sintaxis correcta y capacidades de tu SGBD.

        self._db.execute(q, list(idiomas_origen) +  list(idiomas_destino) + list(textos_origen))
        traducciones = self._db.fetchall()

        # Crear un mapa para acceder fácilmente a las traducciones
        mapa_traducciones = {(row[0].upper().strip(), row[1].upper().strip(), row[2].upper().strip()): row[3] for row in traducciones}

        # Procesar los resultados
        final = []
        for row in data:
            clave = (row['l_ori'].upper().strip(), row['l_dest'].upper().strip(), row['text_ori'].upper().strip())
            if clave in mapa_traducciones:
                row['text_dest'] = mapa_traducciones[clave]
            final.append(row) if row not in final else None
        return final

    def detect_language(self,text):
        return super().detect_language(text,self.thread_id)


    def translate_save(self,l_ori,l_dest,text_ori):
        if l_ori.upper() == 'DETECT LANGUAGE':
            l_ori = self.detect_language(text_ori)
        text_dest = self.get_translation(l_ori,l_dest,text_ori)
        if text_dest:
            return text_dest
        text_dest = super().translate(l_ori,l_dest,text_ori,self.thread_id)
        patron = r'is "(.*?)"'
        resultados = re.findall(patron, text_dest)
        if not resultados:
            patron = r'is (.*?)'
            resultados = re.findall(patron, text_dest)
        text_dest = resultados[0] if resultados else text_dest
        print(f"Traduction done {l_ori},{l_dest},{text_ori},{text_dest}")
        self.save_translation(l_ori,l_dest,text_ori,text_dest)
        return text_dest

    def translate_save_batch(self,data:list[dict[str:str]]):
        super()._write_log("Translating batch "+str(len(data)))
        final = []
        for row in data:
            if row["l_ori"] == 'DETECT LANGUAGE':
                row["l_ori"] = self.detect_language(row["text_ori"])

        data_dest = self.get_batch_translations(data)
        super()._write_log("Translations found: "+str(len(data_dest)))

        # Identificar los textos que aún necesitan traducción
        to_translate = [row for row in data_dest if "text_dest" not in row.keys() or row["text_dest"] is None or row["text_dest"] == ""]
        super()._write_log("Texts to translate: "+str(len(to_translate)))
        to_save=[]
        # Traducir los que no se encontraron en la base de datos
        for row in to_translate:
            now = time.time()
            text_dest = self.get_translation(row["l_ori"], row["l_dest"], row["text_ori"])
            if text_dest:
                row["text_dest"] = text_dest
                continue
            text_dest = super().translate(row["l_ori"], row["l_dest"], row["text_ori"], self.thread_id)
            #print("Translation made: text_ori: ", row["text_ori"], "text_dest: ", text_dest, "time: ", time.time() - now,end="\n\n")
            # Extraer el texto traducido del resultado
            patron = r'is "(.*?)"'
            resultados = re.findall(patron, text_dest)
            if not resultados:
                patron = r'is (.*?)'
                resultados = re.findall(patron, text_dest)
            text_dest = resultados[0] if resultados else text_dest

            # Guardar la nueva traducción en la base de datos
            #self.save_translation(row["l_ori"], row["l_dest"], row["text_ori"], text_dest)
            #Optimizamos y guardamos al final
            to_save.append((row["l_ori"], row["l_dest"], row["text_ori"], text_dest))

            # Actualizar el diccionario con la traducción obtenida
            row["texto_dest"] = text_dest

        # Combinar los resultados: los que ya tenían traducción y los nuevos traducidos
        final.extend(data_dest)  # Aquí data_dest ya incluye las traducciones encontradas y las nuevas

        # Guardar las nuevas traducciones en la base de datos
        self.save_batch_translations_overwrite(to_save)

        super()._write_log("Translations made: "+str(len(final)))

        return final



    def query_translations(self,text):
        q = f"SELECT * FROM tbdtraduccionesOpenAI WHERE fldtextoorigen LIKE '%{text}%' or fldtextodestino LIKE '%{text}%'"
        self._db.execute(q)
        res = self._db.fetchall()
        final = []
        for row in res:
            final.append({
                'idioma_origen':row[0],
                'idioma_destino':row[1],
                'texto_origen':row[2],
                'texto_destino':row[3]
            })
        return final

