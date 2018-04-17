package cfb

import (
	"bytes"
	"errors"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"strings"
	"unicode"
)

var MAGIC = []byte{0xD0,0xCF,0x11,0xE0,0xA1,0xB1,0x1A,0xE1}

var ErrorStructure = errors.New("Not an OLE2 structured storage file")

//constants for Sector IDs (from AAF specifications)
const (
	MAXREGSECT uint32 = 0xFFFFFFFA // (-6) maximum SECT
	DIFSECT    uint32 = 0xFFFFFFFC // (-4) denotes a DIFAT sector in a FAT
	FATSECT    uint32 = 0xFFFFFFFD // (-3) denotes a FAT sector in a FAT
	ENDOFCHAIN uint32 = 0xFFFFFFFE // (-2) end of a virtual stream chain
	FREESECT   uint32 = 0xFFFFFFFF // (-1) unallocated sector
)

//constants for Directory Entry IDs (from AAF specifications)
const (
	MAXREGSID  uint32 = 0xFFFFFFFA // (-6) maximum directory entry ID
	NOSTREAM   uint32 = 0xFFFFFFFF // (-1) unallocated directory entry
)

//object types in storage (from AAF specifications)
const (
	STGTY_EMPTY     uint8 = 0 // empty directory entry
	STGTY_STORAGE   uint8 = 1 // element is a storage object
	STGTY_STREAM    uint8 = 2 // element is a stream object
	STGTY_LOCKBYTES uint8 = 3 // element is an ILockBytes object
	STGTY_PROPERTY  uint8 = 4 // element is an IPropertyStorage object
	STGTY_ROOT      uint8 = 5 // element is a root storage
)

//Unknown size for a stream (used by OleStream):
const UNKNOWN_SIZE uint32 = 0x7FFFFFFF

//=================== FUNCTIONS ================================

func IsOleFile (filename string) (bool, error) {
	f, err := NewFile(filename)
	if err != nil {
		return false, err
	}
	if f.Size() < len(MAGIC) {
		return false, nil
	}
	header, err := f.ReadBytes(len(MAGIC))
	if err != nil {
		return  false, err
	}
	if !bytes.Equal(MAGIC, header){
		return false, nil
	}
	return true, nil
}

//=================== CLASSES ================================

//------------------- OleFile -------------------

type headerFields struct {
	Signature           [8]byte
	CLSID               [16]byte    //CLSID - ignore, must be null
	MinorVersion        uint16      //Version number for non-breaking changes. This field SHOULD be set to 0x003E if the major version field is either 0x0003 or 0x0004.
	MajorVersion        uint16      //Version number for breaking changes. This field MUST be set to either 0x0003 (version 3) or 0x0004 (version 4).
	ByteOrder           uint16      //byte order - ignore, must be little endian
	SectorShift         uint16      //This field MUST be set to 0x0009, or 0x000c, depending on the Major Version field. This field specifies the sector size of the compound file as a power of 2. If Major Version is 3, then the Sector Shift MUST be 0x0009, specifying a sector size of 512 bytes. If Major Version is 4, then the Sector Shift MUST be 0x000C, specifying a sector size of 4096 bytes.
	MiniSectorShift     uint16      // ministream sector size - ignore, must be 64 bytes
	Reserved            [6]byte     // reserved - ignore, not used
	NumDirSectors       uint32      //This integer field contains the count of the number of directory sectors in the compound file. If Major Version is 3, then the Number of Directory Sectors MUST be zero. This field is not supported for version 3 compound files.
	NumFatSectors       uint32      //This integer field contains the count of the number of FAT sectors in the compound file.
	FirstDirSector      uint32      //This integer field contains the starting sector number for the directory stream.
	TransactionSignatureNumber uint32 	// transaction - ignore, not used
	MiniStreamCutoffSize       uint32   // mini stream size cutooff - ignore, must be 4096 bytes
	FirstMiniFatSector  uint32      //This integer field contains the starting sector number for the mini FAT.
	NumMiniFatSectors   uint32      //This integer field contains the count of the number of mini FAT sectors in the compound file.
	FirstDifatSector    uint32      //This integer field contains the starting sector number for the DIFAT.
	NumDifatSectors     uint32      //This integer field contains the count of the number of DIFAT sectors in the compound file.
	Difats              [109]uint32 //The first 109 difat sectors are included in the header
}


type OleFile struct {
	//self.ministream = None
	used_streams_fat     []uint32
	used_streams_minifat []uint32
	directory_fp *OleStream
	direntries []*OleDirectoryEntry

	fat []uint32
	fp *File
	header *headerFields
	//self.metadata = None
	mini_sector_size uint32
	minifat []uint32
	//self.minifatsect = None
	//self.minisectorcutoff = None
	//self.minisectorsize = None
	ministream *OleStream
	nb_sect int
	root *OleDirectoryEntry
	sector_size uint32
}

func NewOleFile(filename string) (this *OleFile, err error) {
	this = &OleFile{
		used_streams_fat: make([]uint32, 0, 10),
		used_streams_minifat: make([]uint32, 0, 10),
	}
	if filename != "" {
		if err = this.Open(filename); err != nil {
			this = nil
		}
	}
	return
}

func (this *OleFile)Open(filename string) (err error) {
	if this.fp, err = NewFile(filename); err != nil {
		return
	}
	this.header = new(headerFields)
	err = binary.Read(this.fp, binary.LittleEndian, this.header)
	if err != nil {
		return
	}
	if !bytes.Equal(MAGIC, this.header.Signature[:]){
		err = ErrorStructure
		return
	}
	if this.header.MajorVersion !=3 && this.header.MajorVersion != 4 {
		err = fmt.Errorf("Incorrect Version in OLE header: %v", this.header.MajorVersion)
		return
	}
	if this.header.ByteOrder != 0xFFFE {
		err = fmt.Errorf("Incorrect ByteOrder in OLE header: %v", this.header.ByteOrder)
		return
	}
	this.sector_size = uint32(1 << this.header.SectorShift)
	if this.sector_size != 512 && this.sector_size != 4096 {
		err = fmt.Errorf("Incorrect sector_size in OLE header (expected: 512 or 4096): %v", this.sector_size)
		return
	}
	if (this.header.MajorVersion==3 && this.sector_size !=512) ||
		(this.header.MajorVersion==4 && this.sector_size !=4096) {
		err = errors.New("sector_size does not match DllVersion in OLE header")
		return
	}
	this.mini_sector_size = uint32(1 << this.header.MiniSectorShift)
	if this.mini_sector_size != 64 {
		err = fmt.Errorf("Incorrect mini_sector_size in OLE header: %v", this.mini_sector_size)
		return
	}
	if this.sector_size==512 && this.header.NumDirSectors!=0 {
		err = errors.New("Incorrect number of directory sectors in OLE header")
		return
	}
	if this.header.TransactionSignatureNumber != 0 {
		err = errors.New("Incorrect OLE header (transaction_signature_number>0)")
		return
	}
	if this.header.MiniStreamCutoffSize != 0x1000 {
		err = errors.New("Incorrect mini_stream_cutoff_size in OLE header")
		return
	}
	// calculate the number of sectors in the file
	// (-1 because header doesn't count)
	this.nb_sect = ((this.fp.Size() + int(this.sector_size)-1) / int(this.sector_size)) - 1

	if err = this.check_duplicate_stream(this.header.FirstDirSector, false); err != nil{
		return
	}
	if this.header.NumMiniFatSectors > 0 {
		if err = this.check_duplicate_stream(this.header.FirstMiniFatSector, false); err != nil{
			return
		}
	}
	if this.header.NumDifatSectors > 0 {
		if err = this.check_duplicate_stream(this.header.FirstDifatSector, false); err != nil{
			return
		}
	}

	// Load file allocation tables
	if err = this.loadfat(); err != nil {
		return
	}
	// Load directory.  This sets both the direntries list (ordered by sid)
	// and the root (ordered by hierarchy) members.
	if err = this.loaddirectory(this.header.FirstDirSector); err != nil {
		return
	}
	//this.minifatsect = this.header.FirstMiniFatSector

	return
}

func (this *OleFile)Close()  {
	if this != nil && this.fp != nil {
		this.fp.Close()
	}
}

func (this *OleFile)check_duplicate_stream(first_sect uint32, minifat bool) error {
	var used_streams []uint32
	if minifat {
		used_streams = this.used_streams_minifat
	} else {
		if first_sect == DIFSECT || first_sect == FATSECT || first_sect == ENDOFCHAIN || first_sect == FREESECT {
			return nil
		}
		used_streams = this.used_streams_fat
	}

	for i := 0; i < len(used_streams); i++ {
		if used_streams[i] == first_sect {
			return errors.New("Stream referenced twice")
		}
	}
	used_streams = append(used_streams, first_sect)

	return nil
}

//Load the FAT table.
func (this *OleFile)loadfat() (err error) {
	logger.Debug("Loading the FAT table, starting with the 1st sector after the header")

	this.fat = make([]uint32, 0)

	if _, err = this.loadfat_sect(this.header.Difats[:]); err != nil {
		return
	}
	if this.header.NumDifatSectors != 0 {
		logger.Debug("DIFAT is used, because file size > 6.8MB.")
		if this.header.NumFatSectors <= 109{
			// there must be at least 109 blocks in header and the rest in
			// DIFAT, so number of sectors must be >109.
			err = errors.New("Incorrect DIFAT, not enough sectors")
			return
		}
		if int(this.header.FirstDifatSector) >= this.nb_sect{
			// initial DIFAT block index must be valid
			err = errors.New("Incorrect DIFAT, first index out of range")
			return
		}
		logger.Debug( "DIFAT analysis..." )
		nb_difat_sectors := (this.sector_size/4)-1
		nb_difat := (this.header.NumFatSectors-109 + nb_difat_sectors-1)/nb_difat_sectors
		logger.Debugf( "nb_difat = %d\n", nb_difat )
		if this.header.NumDifatSectors != nb_difat{
			err = errors.New("Incorrect DIFAT")
			return
		}
		isect_difat := this.header.FirstDifatSector
		for i := uint32(0); i < nb_difat; i++ {
			var sector_difat []byte
			var difat []uint32

			logger.Debugf( "DIFAT block %d, sector %X\n", i, isect_difat )
			if sector_difat, err = this.getsect(isect_difat); err != nil{
				return
			}
			if difat, err = sect2array(sector_difat); err != nil{
				return
			}
			if _, err = this.loadfat_sect(difat[:nb_difat_sectors]); err != nil{
				return
			}
			isect_difat = difat[nb_difat_sectors]
			logger.Debugf( "next DIFAT sector: %X\n", isect_difat )
		}
		if isect_difat != ENDOFCHAIN && isect_difat != FREESECT{
			err = errors.New("Incorrect end of DIFAT")
			return
		}
	}else{
		logger.Debug("No DIFAT, because file size < 6.8MB.")
	}

	if len(this.fat) > this.nb_sect{
		logger.Debugf("len(fat)=%d, shrunk to nb_sect=%d\n", len(this.fat), this.nb_sect)
		this.fat = this.fat[:this.nb_sect]
	}
	logger.Debugf("FAT references %d sectors / Maximum %d sectors in file", len(this.fat), this.nb_sect)

	return
}

// Adds the indexes of the given sector to the FAT
func (this *OleFile)loadfat_sect(sect []uint32) (isect uint32, err error) {
	for _, isect = range sect{
		var s []byte
		var nextfat []uint32

		isect = isect & 0xFFFFFFFF  // JYTHON-WORKAROUND
		logger.Debugf("isect = %X\n", isect)
		if isect == ENDOFCHAIN || isect == FREESECT{
			// the end of the sector chain has been reached
			logger.Debug("found end of sector chain")
			break
		}
		// read the FAT sector
		if s, err = this.getsect(isect); err != nil{
			return
		}
		// parse it as an array of 32 bits integers, and add it to the
		// global FAT array
		if nextfat, err = sect2array(s); err != nil{
			return
		}
		this.fat = append(this.fat, nextfat...)
	}
	return
}

// Read given sector from file on disk.
func (this *OleFile)getsect(sect uint32) (sector []byte, err error)  {
	if _, err = this.fp.Seek(int(this.sector_size * (sect+1)), 0); err != nil{
		return
	}
	sector = make([]byte, this.sector_size)
	var n int
	if n, err = this.fp.Read(sector); err != nil{
		return
	}
	if n != int(this.sector_size) {
		err = errors.New("Incomplete OLE sector")
	}
	return
}

// Load the directory.
func (this *OleFile)loaddirectory(sect uint32) (err error) {
	logger.Debug("Loading the Directory:")

	if this.directory_fp, err = this.open(sect, UNKNOWN_SIZE, true); err != nil{
		return
	}
	max_entries := this.directory_fp.size / 128

	logger.Debugf("loaddirectory: size=%d, max_entries=%d",
		this.directory_fp.size, max_entries)

	this.direntries = make([]*OleDirectoryEntry, max_entries)

	if _, err = this.load_direntry(0); err != nil {
		return
	}
	this.root = this.direntries[0]
	this.root.build_storage_tree()
	return
}

// Load a directory entry from the directory.
func (this *OleFile)load_direntry (sid int)  (de *OleDirectoryEntry, err error) {
	if sid < 0 || sid >= len(this.direntries) {
		err = errors.New("OLE directory index out of range")
		return
	}
	if this.direntries[sid] != nil {
		err = errors.New("double reference for OLE stream/storage")
		return
	}
	this.directory_fp.Seek(sid * 128, 0)
	entry := make([]byte, 128)
	if _, err = this.directory_fp.Read(entry); err != nil {
		return
	}
	this.direntries[sid], err = NewOleDirectoryEntry(entry, sid, this)
	de = this.direntries[sid]
	return
}

// Open a stream, either in FAT or MiniFAT according to its size.
func (this *OleFile)open(start, size uint32, force_FAT bool) (stream *OleStream, err error) {
	logger.Debugf("OleFile.open(): sect=%Xh, size=%d, force_FAT=%v",
		start, size, force_FAT)

	if size < this.header.MiniStreamCutoffSize && !force_FAT{
		// ministream object
		if this.ministream == nil {
			if err = this.loadminifat(); err != nil {
				return
			}
			size_ministream := this.root.size
			logger.Debugf("Opening MiniStream: sect=%Xh, size=%d",
				this.root.header.IsectStart, size_ministream)
			this.ministream, err = this.open(this.root.header.IsectStart, uint32(size_ministream), true)
			if err != nil {
				this.ministream = nil
				return
			}
		}
		stream, err = NewOleStream(this.ministream, start, size,
			0, this.mini_sector_size,
			this.minifat, uint32(this.ministream.size),
			this)
	}else{
		// standard stream
		stream, err = NewOleStream(this.fp, start, size,
			this.sector_size,
			this.sector_size, this.fat,
			uint32(this.fp.Size()),
			this)
	}
	return
}

// Load the MiniFAT table.
func (this *OleFile)loadminifat() (err error) {
	stream_size := this.header.NumMiniFatSectors * this.sector_size
	nb_minisectors := (this.root.size + uint64(this.mini_sector_size)-1) / uint64(this.mini_sector_size)
	used_size := nb_minisectors * 4
	logger.Debugf("loadminifat(): minifatsect=%d, nb FAT sectors=%d, used_size=%d, stream_size=%d, nb MiniSectors=%d",
		this.header.FirstMiniFatSector, this.header.NumMiniFatSectors, used_size, stream_size, nb_minisectors)

	if used_size > uint64(stream_size) {
		//This is not really a problem, but may indicate a wrong implementation:
		err = errors.New("OLE MiniStream is larger than MiniFAT")
		return
	}
	var s *OleStream
	if s, err = this.open(this.header.FirstMiniFatSector, stream_size, true); err != nil{
		return
	}
	var b []byte
	if b, err = s.read(); err != nil{
		return
	}
	if this.minifat, err = sect2array(b); err != nil{
		return
	}
	logger.Debugf("MiniFAT shrunk from %d to %d sectors", len(this.minifat), nb_minisectors)
	this.minifat = this.minifat[:nb_minisectors]
	logger.Debugf("loadminifat(): len=%d", len(this.minifat))

	return
}

//Test if given filename exists as a stream or a storage in the OLE
//container.
func (this *OleFile)Exists(filename string) bool {
	if _, err := this.find(strings.ToLower(filename)); err != nil {
		return false
	}
	return true
}


//Returns directory entry of given filename. (openstream helper)
//Note: this method is case-insensitive.
func (this *OleFile)find(filename string) (sid int, err error) {
	filenames := strings.Split(filename, "/")
	node := this.root
	for _, name := range filenames {
		var de *OleDirectoryEntry
		for _, kid := range node.kids {
			if strings.ToLower(kid.name) == name {
				de = kid
				break
			}
		}
		if de != nil{
			node = de
		}else{
			return -1, errors.New("file not found")
		}
	}
	return node.sid, nil
}

//Test if given filename exists as a stream or a storage in the OLE
//container, and return its type.
func (this *OleFile)GetType(filename string) uint8 {
	sid, err := this.find(filename)
	if err != nil {
		return STGTY_EMPTY
	}
	entry, err := this.get_direntry(sid)
	if err != nil {
		return STGTY_EMPTY
	}
	return entry.header.EntryType
}

//Return size of a stream in the OLE container, in bytes.
func (this *OleFile)GetSize(filename string) int {
	sid, err := this.find(filename)
	if err != nil {
		return -1
	}
	entry, err := this.get_direntry(sid)
	if err != nil {
		return -1
	}
	if entry.header.EntryType != STGTY_STREAM {
		return -1
	}
	return int(entry.size)
}

//Return root entry name. Should usually be 'Root Entry' or 'R' in most
//implementations.
func (this *OleFile)GetRootentryName() string {
	return this.root.name
}

func (this *OleFile)get_direntry(sid int) (de *OleDirectoryEntry, err error) {
	if sid < 0 || sid >= len(this.direntries) {
		err = errors.New("Direntry: out of range")
		return
	}
	de = this.direntries[sid]
	return
}

//Open a stream as a read-only file object (BytesIO).
func (this *OleFile)OpenStream(filename string) (*OleStream, error) {
	sid, err := this.find(filename)
	if err != nil {
		return nil, err
	}
	entry, err := this.get_direntry(sid)
	if err != nil {
		return nil, err
	}


	if entry.header.EntryType != STGTY_STREAM{
		return nil, errors.New("This file is not a stream")
	}
	return this.open(entry.header.IsectStart,uint32(entry.size), false)
}

func (this *OleFile)GetHeader() *headerFields {
	return this.header
}

//Write a stream to disk. For now, it is only possible to replace an
//existing stream by data of the same size.
func (this *OleFile)WriteStream(stream_name string, data []byte) (err error) {
	if data == nil {
		err = errors.New("write_stream: data must be a bytes string")
		return
	}
	var sid int
	if sid, err = this.find(stream_name); err != nil {
		return
	}
	var entry *OleDirectoryEntry
	if entry, err = this.get_direntry(sid); err != nil {
		return
	}
	if entry.header.EntryType != STGTY_STREAM {
		err = errors.New("this is not a stream")
		return
	}
	size := int(entry.size)
	if size != len(data) {
		err = errors.New("write_stream: data must be the same size as the existing stream")
		return
	}
	if size < int(this.header.MiniStreamCutoffSize)  && entry.header.EntryType != STGTY_ROOT {
		return this.write_mini_stream(entry, data)
	}

	sect := entry.header.IsectStart
	// number of sectors to write
	nb_sectors := (size + (int(this.sector_size)-1)) // self.sectorsize
	logger.Debugf("b_sectors = %d", nb_sectors)
	for i := 0; i < nb_sectors; i++ {
		var data_sector []byte
		if i<(nb_sectors-1) {
			data_sector = data [i*int(this.sector_size) : (i+1)*int(this.sector_size)]
			if len(data_sector)==int(this.sector_size) {
				err = errors.New("Error len data_sector")
				return
			}
		}else {
			data_sector = data [i*int(this.sector_size):]
			logger.Debugf("write_stream: size=%d sectorsize=%d data_sector=%Xh size%%sectorsize=%d",
			                    size, this.sector_size, len(data_sector), size % int(this.sector_size))
		}
		if err = this.write_sect(sect, data_sector); err != nil {
			return
		}

		if sect, err = this.get_fat(sect); err != nil {
			return
		}
	}
	if sect != ENDOFCHAIN{
		err = errors.New("incorrect last sector index in OLE stream")
	}
	return
}

func (this *OleFile)get_fat(id uint32) (uint32, error) {
	if int(id) >= len(this.fat) {
		return 0, errors.New("incorrect OLE FAT, sector index out of range")
	}
	return this.fat[id], nil
}

func (this *OleFile)get_minifat(id uint32) (uint32, error) {
	if int(id) >= len(this.minifat) {
		return 0, errors.New("incorrect OLE MiniFAT, sector index out of range")
	}
	return this.minifat[id], nil
}

func (this *OleFile)write_mini_stream(entry *OleDirectoryEntry, data_to_write []byte) error {
	if entry.sect_chain == nil {
		if err := entry.build_sect_chain(this); err != nil {
			return err
		}
	}
	nb_sectors := len(entry.sect_chain)

	if this.root.sect_chain == nil {
		if err := this.root.build_sect_chain(this); err != nil {
			return err
		}
	}
	block_size := this.sector_size / this.mini_sector_size
	for idx, sect := range entry.sect_chain {
		sect_base := sect / block_size
		sect_offset := sect % block_size
		fp_pos := (this.root.sect_chain[sect_base]+1)*this.sector_size + sect_offset*this.mini_sector_size
		var data_per_sector []byte
		if idx < (nb_sectors - 1) {
			data_per_sector = data_to_write[idx*int(this.mini_sector_size) : (idx+1)*int(this.mini_sector_size)]
		}else {
			data_per_sector = data_to_write[idx*int(this.mini_sector_size):]
		}
		if err := this.write_mini_sect(fp_pos, data_per_sector); err != nil{
			return err
		}
	}
	return nil
}

//Write given sector to file on disk.
func (this *OleFile) write_sect(sect uint32, data []byte) error {
	if data == nil {
		return errors.New("write_sect: data must be a bytes string")
	}

	if _, err := this.fp.Seek(int(this.sector_size*(sect+1)), 0); err != nil {
		return err
	}

	if len(data) > int(this.sector_size) {
		return errors.New("Data is larger than sector size")
	}
	_, err := this.fp.Write(data)
	return err
}

//Write given sector to file on disk.
func (this *OleFile) write_mini_sect(fp_pos uint32, data []byte) error {
	if data == nil {
		return errors.New("write_sect: data must be a bytes string")
	}

	if _, err := this.fp.Seek(int(fp_pos), 0); err != nil {
		return err
	}

	len_data := len(data)
	if int(this.mini_sector_size) < len_data {
		return errors.New("Data is larger than sector size")
	}
	_, err := this.fp.Write(data)
	return err
}

func (this *OleFile) Save() error {
	return this.fp.Save()
}

//------------------- OleStream -------------------

type Stream interface {
	Seek(offset int, whence int) (int, error)
	Read(b []byte) (n int, err error)
	Close()
} 

type OleStream struct {
	ole *OleFile
	size int
	data []byte
	off int
}

func NewOleStream(fp Stream, sect, size , offset, sectorsize uint32, fat []uint32, filesize uint32, olefileio *OleFile) (this *OleStream, err error) {
	logger.Debugf("  sect=%d (%X), size=%d, offset=%d, sectorsize=%d, len(fat)=%d, fp=%v",
            sect,sect,size,offset,sectorsize,len(fat), fp)

	this = new(OleStream)
	this.ole = olefileio
	if this.ole == nil || this.ole.fp == nil || this.ole.fp.Size() <= 0 {
		err = errors.New("Attempting to open a stream from a closed OLE File")
		return
	}
	unknown_size := false
	if size == UNKNOWN_SIZE {
		// this is the case when called from OleFileIO._open(), and stream
		// size is not known in advance (for example when reading the
		// Directory stream). Then we can only guess maximum size:
		size = uint32(len(fat)) * sectorsize
		// and we keep a record that size was unknown:
		unknown_size = true
		logger.Debug("  stream with UNKNOWN SIZE")
	}
	nb_sectors := (size + (sectorsize-1)) // sectorsize
	logger.Debugf("nb_sectors = %d", nb_sectors)
	//if int(nb_sectors) > len(fat) {
	//	err = errors.New("malformed OLE document, stream too large")
	//	return
	//}
	if size == 0 && sect != ENDOFCHAIN{
		err = errors.New("incorrect OLE sector index for empty stream")
		return
	}
	buf := new(bytes.Buffer)
	for i := uint32(0); i < nb_sectors; i++ {
		logger.Debugf("Reading stream sector[%d] = %Xh", i, sect)
		if sect == ENDOFCHAIN {
			if unknown_size {
				logger.Debug("Reached ENDOFCHAIN sector for stream with unknown size")
			} else {
				logger.Debug("Reached ENDOFCHAIN sector for stream with known size. Incomplete OLE stream")
			}
			break
		}
		// sector index should be within FAT:
		if sect<0 || int(sect) >= len(fat){
			err = errors.New("incorrect OLE FAT, sector index out of range")
			return
		}
		if _, err = fp.Seek(int(offset + sectorsize * sect), 0); err != nil {
			return
		}
		sector_data := make([]byte, sectorsize)
		var n int
		if n, err = fp.Read(sector_data); err != nil {
			return
		}
		if n != int(sectorsize) && int(sect)!=(len(fat)-1){
			err = errors.New("incomplete OLE sector")
			return
		}
		buf.Write(sector_data)

		if int(sect) < len(fat) {
			sect = fat[sect] & 0xFFFFFFFF // JYTHON-WORKAROUND
		}else{
			err = errors.New("incorrect OLE FAT, sector index out of range")
			return
		}
	}
	this.data = buf.Bytes()
	if len(this.data) >= int(size) {
		this.data = this.data[:size]
		this.size = int(size)
	}else if unknown_size {
		this.size = len(this.data)
	}else{
		err = errors.New("OLE stream size is less than declared")
	}
	return
}

func (this *OleStream) Read(b []byte) (n int, err error) {
	if this == nil {
		return 0, nil
	}
	if this.off >= len(this.data) {
		return 0, io.EOF
	}
	n = copy(b, this.data[this.off:])
	this.off += n
	return
}

func (f *OleStream) Seek(offset int, whence int) (int, error) {
	switch whence {
	case io.SeekStart:
		f.off = offset
	case io.SeekCurrent:
		f.off += offset
	case io.SeekEnd:
		f.off = len(f.data) + offset
	}
	return f.off, nil
}

func (this *OleStream)read() ([]byte, error) {
	if this.data == nil {
		return nil, errors.New("OleStream: Read error")
	}
	return this.data, nil
}

func (this *OleStream)Size() int {
	if this == nil {
		return 0
	}
	return this.size
}

func (this *OleStream)Close() {
	if this == nil {
		return
	}
	this.size = 0
	this.ole = nil
	this.data = this.data[:0]
	this.off = 0
}

//------------------- OleDirectoryEntry -------------------

const DIRENTRY_SIZE = 128

type directoryEntryFields struct {
	NameRaw           [32]uint16     //64 bytes, unicode string encoded in UTF-16. If root, "Root Entry\0" w
	NameLength        uint16         //2 bytes
	EntryType         uint8          //1 byte Must be one of the types specified above
	Color             uint8          //1 byte Must be 0x00 RED or 0x01 BLACK
	SidLeft           uint32         //4 bytes, Dir? Stream ID of left sibling, if none set to NOSTREAM
	SidRight          uint32         //4 bytes, Dir? Stream ID of right sibling, if none set to NOSTREAM
	SidChild          uint32         //4 bytes, Dir? Stream ID of child object, if none set to NOSTREAM
	Clsid             Guid           // Contains an object class GUID (must be set to zeroes for stream object)
	DwUserFlags       [4]byte        // user-defined flags for storage object
	CreateTime        FileTime       // Windows FILETIME structure
	ModifyTime        FileTime       // Windows FILETIME structure
	IsectStart 		  uint32         // if a stream object, first sector location. If root, first sector of ministream
	Size              [8]byte
}

type OleDirectoryEntry struct {
	sid int
	olefile *OleFile
	used bool
	kids []*OleDirectoryEntry
	kids_dict map[string]*OleDirectoryEntry
	header *directoryEntryFields
	name_utf16 []uint16
	name string
	size uint64
	clsid string
	is_minifat bool
	sect_chain []uint32
}

func NewOleDirectoryEntry(entry []byte, sid int, olefile *OleFile) (this *OleDirectoryEntry, err error) {
	this = &OleDirectoryEntry{
		sid: sid,
		olefile: olefile,
		header: new(directoryEntryFields),
		kids: make([]*OleDirectoryEntry, 0),
		kids_dict: make(map[string]*OleDirectoryEntry),
	}
	r := bytes.NewBuffer(entry)
	if err = binary.Read(r, binary.LittleEndian, this.header); err != nil {
		return
	}
	if this.header.EntryType != STGTY_ROOT && this.header.EntryType != STGTY_STORAGE &&
		this.header.EntryType != STGTY_STREAM && this.header.EntryType != STGTY_EMPTY {
		err = errors.New("unhandled OLE storage type")
		return
	}
	if this.header.EntryType == STGTY_ROOT && sid != 0 {
		err = errors.New("duplicate OLE root entry")
		return
	}
	if sid == 0 && this.header.EntryType != STGTY_ROOT {
		err = errors.New("incorrect OLE root entry")
		return
	}
	if this.header.NameLength>64 {
		err = errors.New("incorrect DirEntry name length >64 bytes")
		return
	}

	if unicode.IsPrint(rune(this.header.NameRaw[0])) {
		this.name_utf16 = this.header.NameRaw[:(this.header.NameLength/2-1)]
	}else{
		this.name_utf16 = this.header.NameRaw[1:(this.header.NameLength/2-1)]
	}

	this.name = decode_utf16_str(this.name_utf16)

	logger.Debugf("DirEntry SID=%d: %s", this.sid, this.name)
	logger.Debugf(" - type: %d", this.header.EntryType)
	logger.Debugf(" - sect: %Xh", this.header.IsectStart)
	logger.Debugf(" - SID left: %d, right: %d, child: %d", this.header.SidLeft,
		this.header.SidRight, this.header.SidChild)

	if olefile.sector_size == 512{
		this.size = uint64(binary.LittleEndian.Uint32(this.header.Size[:4]))
	}else{
		this.size = binary.LittleEndian.Uint64(this.header.Size[:])
	}
	logger.Debugf(" - size: %d (size=%v)", this.size, this.header.Size)

	this.clsid = this.header.Clsid.String()

	if this.header.EntryType == STGTY_STORAGE && this.size != 0 {
		err = errors.New("OLE storage with size>0")
		this.is_minifat = false
		return
	}
	if (this.header.EntryType == STGTY_ROOT || this.header.EntryType ==  STGTY_STREAM) && this.size > 0 {
		if this.size < uint64(olefile.header.MiniStreamCutoffSize) && this.header.EntryType == STGTY_STREAM {
			this.is_minifat = true
		} else {
			this.is_minifat = false
		}
		olefile.check_duplicate_stream(this.header.IsectStart, this.is_minifat)
	}
	this.sect_chain = nil
	return
}

//Read and build the red-black tree attached to this OleDirectoryEntry
//object, if it is a storage.
func (this *OleDirectoryEntry) build_storage_tree() (err error) {
	logger.Debugf("build_storage_tree: SID=%d - %s - sid_child=%d", this.sid, this.name, this.header.SidChild)
	if this.header.SidChild != NOSTREAM {
		if err = this.append_kids(this.header.SidChild); err != nil {
			return
		}
		sort.Slice(this.kids, func(i, j int) bool {
			return this.kids[i].name < this.kids[j].name
		})
	}
	return
}

//Walk through red-black tree of children of this directory entry to add
//all of them to the kids list. (recursive method)
func (this *OleDirectoryEntry) append_kids(child_sid uint32) (err error) {
	logger.Debugf("append_kids: child_sid=%d", child_sid)

	if child_sid == NOSTREAM {
		return
	}

	if child_sid < 0 || int(child_sid) >= len(this.olefile.direntries) {
		err = errors.New("OLE DirEntry index out of range")
		return
	} else {
		var child *OleDirectoryEntry
		if child, err = this.olefile.load_direntry(int(child_sid)); err != nil {
			return err
		}
		logger.Debugf("append_kids: child_sid=%d - %s - sid_left=%d, sid_right=%d, sid_child=%d",
			child.sid, child.name, child.header.SidLeft, child.header.SidRight, child.header.SidChild)

		if err = this.append_kids(child.header.SidLeft); err != nil {
			return
		}
		name_lower := strings.ToLower(child.name)

		_, ok := this.kids_dict[name_lower]

		if ok {
			err = errors.New("Duplicate filename in OLE storage")
			return
		}
		this.kids = append(this.kids, child)
		this.kids_dict[name_lower] = child
		if child.used {
			err = errors.New("OLE Entry referenced more than once")
			return
		}
		child.used = true
		if this.append_kids(child.header.SidRight); err != nil {
			return
		}
		child.build_storage_tree()
	}
	return
}

func (this *OleDirectoryEntry) build_sect_chain(olefile *OleFile) (err error) {
	if this.sect_chain != nil {
		return
	}
	if (this.header.EntryType != STGTY_ROOT &&
		this.header.EntryType != STGTY_STREAM) ||
		this.size == 0 {
		return
	}

	this.sect_chain = make([]uint32, 0)

	if this.is_minifat && olefile.minifat == nil {
		olefile.loadminifat()
	}

	next_sect := this.header.IsectStart
	for	next_sect != ENDOFCHAIN {
		this.sect_chain = append(this.sect_chain, next_sect)
		if this.is_minifat {
			next_sect, err = olefile.get_minifat(next_sect)
			if err != nil {
				return
			}
		}else {
			next_sect, err = olefile.get_fat(next_sect)
			if err != nil {
				return
			}
		}
	}
	return
}

