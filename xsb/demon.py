import os
os.chdir('/home/zx/workspace/recommendor/spotlight')

import numpy as np
import zipfile
import operator
import math
from lightfm import LightFM
from lightfm.evaluation import precision_at_k
from lightfm.evaluation import auc_score
from lightfm.evaluation import *
import psycopg2
import operator
from lightfm.evaluation import auc_score
from lightfm import cross_validation
from lightfm.data import Dataset
import pickle
import torch
from spotlight.interactions import Interactions
from spotlight.factorization.implicit import ImplicitFactorizationModel
from spotlight.evaluation import rmse_score
from spotlight.evaluation import mrr_score
from spotlight.factorization._components import (_predict_process_features,
                                                 _predict_process_ids)
import sklearn
import torch.nn as nn
import torch.nn.functional as F