import h5py

filename = 'invokestree.hdf5'
f = h5py.File('database/'+filename)
f.create_group('calltree')
