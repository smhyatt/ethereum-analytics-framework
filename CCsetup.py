import h5py

contCFilename = 'contcdata.hdf5'

f = h5py.File('database/CC/'+contCFilename)

ownership = f.create_group('ownership')
# for the ones that are contracts creating contracts where the EOA is not yet seen
ownership.create_group('fallouts')
