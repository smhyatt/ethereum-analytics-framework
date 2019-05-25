import h5py
for i in range(27):
    tokenFilename = 'tokendata'+str(i)+'.hdf5'
    transFilename = 'transdata'+str(i)+'.hdf5'
    callsFilename = 'callsdata'+str(i)+'.hdf5'
    contCFilename = 'contcdata'+str(i)+'.hdf5'

    f1 = h5py.File('database/'+tokenFilename)
    f2 = h5py.File('database/'+transFilename)
    f3 = h5py.File('database/'+callsFilename)
    f4 = h5py.File('database/'+contCFilename)

    ownership = f4.create_group('ownership')
    # for the ones that are contracts creating contracts where the EOA is not yet seen
    ownership.create_group('fallouts')

    dset1 = f1.create_dataset('tokens', (1,11), maxshape=(None,11), dtype=h5py.special_dtype(vlen=str))
    dset2 = f2.create_dataset('transactions', (1,7), maxshape=(None,7), dtype=h5py.special_dtype(vlen=str))
    dset3 = f3.create_dataset('contractTransfers', (1,11), maxshape=(None,11), dtype=h5py.special_dtype(vlen=str))
