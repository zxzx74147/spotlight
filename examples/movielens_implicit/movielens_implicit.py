import numpy as np
import os

from lightfm.data import Dataset
from spotlight.factorization.implicit import ImplicitFactorizationModel

from spotlight.datasets.movielens import get_movielens_dataset
from spotlight.evaluation import rmse_score

dataset = get_movielens_dataset(variant='100K')
print(dataset)

import torch

from spotlight.factorization.explicit import ExplicitFactorizationModel

model = ImplicitFactorizationModel(loss='bpu',
                                   embedding_dim=128,  # latent dimensionality
                                   n_iter=1,  # number of epochs of training
                                   batch_size=1024,  # minibatch size
                                   l2=1e-9,  # strength of L2 regularization
                                   learning_rate=1e-3,
                                   use_cuda=torch.cuda.is_available())

from spotlight.cross_validation import random_train_test_split

train, test = random_train_test_split(dataset, random_state=np.random.RandomState(42))

print('Split into \n {} and \n {}.'.format(train, test))

model.fit(train, verbose=True)

a =model.predict(test.user_ids, test.item_ids)
print(len(a))

train_rmse = rmse_score(model, train)
np.argsort

dataset = Dataset()