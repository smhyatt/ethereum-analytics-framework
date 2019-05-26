import h5py

filename1 = 'invokestree.hdf5'
filename2 = 'segmentedinvokestree.hdf5'
f1 = h5py.File('database/framework/'+filename1)
f2 = h5py.File('database/framework/'+filename2)
f1.create_group('calltree')
f2.create_group('calltree')
