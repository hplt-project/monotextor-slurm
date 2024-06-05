import zstandard
import sys
import re
import os
import io

langs = ["unk", "ace_Arab","ace_Latn","afr_Latn","als_Latn","amh_Ethi","ara_Arab","asm_Beng","ast_Latn","awa_Deva","ayr_Latn","azb_Arab","azj_Latn","bak_Cyrl","bam_Latn","ban_Latn","bel_Cyrl","bem_Latn","ben_Beng","bho_Deva","bjn_Arab","bjn_Latn","bod_Tibt","bos_Latn","bug_Latn","bul_Cyrl","cat_Latn","ceb_Latn","ces_Latn","cjk_Latn","ckb_Arab","crh_Latn","cym_Latn","dan_Latn","deu_Latn","dik_Latn","dyu_Latn","dzo_Tibt","ell_Grek","eng_Latn","epo_Latn","est_Latn","eus_Latn","ewe_Latn","fao_Latn","fij_Latn","fin_Latn","fon_Latn","fra_Latn","fur_Latn","fuv_Latn","gaz_Latn","gla_Latn","gle_Latn","glg_Latn","grn_Latn","guj_Gujr","hat_Latn","hau_Latn","heb_Hebr","hin_Deva","hne_Deva","hrv_Latn","hun_Latn","hye_Armn","ibo_Latn","ilo_Latn","ind_Latn","isl_Latn","ita_Latn","jav_Latn","jpn_Jpan","kab_Latn","kac_Latn","kam_Latn","kan_Knda","kas_Arab","kas_Deva","kat_Geor","kaz_Cyrl","kbp_Latn","kea_Latn","khk_Cyrl","khm_Khmr","kik_Latn","kin_Latn","kir_Cyrl","kmb_Latn","kmr_Latn","knc_Arab","knc_Latn","kon_Latn","kor_Hang","lao_Laoo","lij_Latn","lim_Latn","lin_Latn","lit_Latn","lmo_Latn","ltg_Latn","ltz_Latn","lua_Latn","lug_Latn","luo_Latn","lus_Latn","lvs_Latn","mag_Deva","mai_Deva","mal_Mlym","mar_Deva","min_Latn","mkd_Cyrl","mlt_Latn","mni_Beng","mos_Latn","mri_Latn","mya_Mymr","nld_Latn","nno_Latn","nob_Latn","npi_Deva","nso_Latn","nus_Latn","nya_Latn","oci_Latn","ory_Orya","pag_Latn","pan_Guru","pap_Latn","pbt_Arab","pes_Arab","plt_Latn","pol_Latn","por_Latn","prs_Arab","quy_Latn","ron_Latn","run_Latn","rus_Cyrl","sag_Latn","san_Deva","sat_Olck","scn_Latn","shn_Mymr","sin_Sinh","slk_Latn","slv_Latn","smo_Latn","sna_Latn","snd_Arab","som_Latn","sot_Latn","spa_Latn","srd_Latn","srp_Cyrl","ssw_Latn","sun_Latn","swe_Latn","swh_Latn","szl_Latn","tam_Taml","taq_Latn","taq_Tfng","tat_Cyrl","tel_Telu","tgk_Cyrl","tgl_Latn","tha_Thai","tir_Ethi","tpi_Latn","tsn_Latn","tso_Latn","tuk_Latn","tum_Latn","tur_Latn","twi_Latn","tzm_Tfng","uig_Arab","ukr_Cyrl","umb_Latn","urd_Arab","uzn_Latn","vec_Latn","vie_Latn","war_Latn","wol_Latn","xho_Latn","ydd_Hebr","yor_Latn","yue_Hant","zho_Hans","zho_Hant","zsm_Latn","zul_Latn"]

THREADS = 10 if "SLURM_CPUS_ON_NODE" not in os.environ else os.environ["SLURM_CPUS_ON_NODE"]

class LangWriter():
    MAX_SIZE = 1e11 # max bytes per batch

    def __init__(self, directory):
        self.dir = directory
        self.bytes_written = 0
        self.num_files = 0
        try:
            os.mkdir(self.dir)
            # create dir, ignore error if exists
        except FileExistsError:
            pass
        self.compressor = zstandard.ZstdCompressor(level=10, threads=10)
        self.writer = None
        self.new_split()

    def new_split(self):
        if self.writer:
            self.writer.close()
        self.bytes_written = 0
        self.num_files += 1
        self.writer = zstandard.open(
                f"{self.dir}/batch_{self.num_files}.jsonl.zst",
                'wb',
                cctx=self.compressor)

    # Write bytes to the shard, if max size is reached, open a new batch
    def write(self, text_bytes):
        if self.bytes_written >= self.MAX_SIZE:
            self.new_split()
        self.writer.write(text_bytes)
        self.bytes_written += len(text_bytes)

    def close(self):
        self.writer.close()

    def __del__(self):
        self.writer.close()

output_dir = sys.argv[1]
input_files = sys.argv[2:]
print(output_dir)
print(input_files)
# compile byte-based regex
lang_re = re.compile(b'"lang": ?\["([a-z]{3}_[A-Z][a-z]{3})",.*\], ?"prob": ?\[([0-9]+\.[0-9]+),[0-9]+\.[0-9]+,[0-9]+\.[0-9]+\],')

# Create all the lang directories
# right now, the langcodes are hardcoded, but it could be a dynamic list
# that creates a new langrwiter every time a new lang is found
lang_files = {}
for lang in langs:
    cur_dir = f"{output_dir}/{lang}"
    lang_files[lang] = LangWriter(cur_dir)

for infile in input_files:
    with zstandard.open(infile, 'rb') as docs_file:
        for line in io.BufferedReader(docs_file):
            # obtain lang without decoding string nor parsing json
            match = lang_re.search(line)
            if match is None:
                print(line.decode('utf-8'), file=sys.stderr)
            try:
                # extract lang code and probability from the capture groups
                lang = match.groups()[0].decode()
                prob = float(match.groups()[1])
            except ValueError as e:
                # in case float cannot be parsed show what regex has captured
                print(line, file=sys.stderr)
                print(match, file=sys.stderr)
                raise e

            # confidence threshold
            if prob < 0.5:
                lang = 'unk'

            # move document to its lang dir
            lang_files[lang].write(line)

# close all files
for f in lang_files.values():
    f.close()
