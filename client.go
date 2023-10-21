package client

import (
	"bytes"
	"io"
	"strings"

	goerr "errors"

	"upspin.io/access"
	"upspin.io/bind"
	clnt "upspin.io/client"
	"upspin.io/errors"
	"upspin.io/flags"
	"upspin.io/metric"
	"upspin.io/pack"
	"upspin.io/path"
	"upspin.io/upspin"
)

// Client API with the option to use a io.Reader for writing data to upspin
type Client interface {
	upspin.Client

	// Write stores the data at the given name, in a streaming manner. Reassembles the same
	// behaviour of upspin.Client#Put.
	// If something is already stored with that name, it will no longer be available using the
	// name, although it may still exist in the storage server. (See
	// the documentation for Delete.) Like Get, it is not the usual
	// access method. The file-like API is preferred.
	//
	// A successful Put returns an incomplete DirEntry (see the
	// description of AttrIncomplete) containing only the
	// new sequence number.
	Write(name upspin.PathName, r io.Reader) (*upspin.DirEntry, error)
}

const (
	followFinalLink = true
)

type client struct {
	config upspin.Config
	c      upspin.Client
}

var _ upspin.Client = (*client)(nil)

// New Client implementation
func New(config upspin.Config) Client {
	return &client{config: config, c: clnt.New(config)}
}

// PutLink implements upspin.Client.
func (c *client) PutLink(oldName, linkName upspin.PathName) (*upspin.DirEntry, error) {
	return c.c.PutLink(oldName, linkName)
}

// Put implements upspin.Client.
func (c *client) Put(name upspin.PathName, data []byte) (*upspin.DirEntry, error) {
	return c.PutSequenced(name, upspin.SeqIgnore, data)
}

// PutSequenced implements upspin.Client.
func (c *client) PutSequenced(name upspin.PathName, seq int64, data []byte) (*upspin.DirEntry, error) {
	const op errors.Op = "client.Put"
	m, s := newMetric(op)
	defer m.Done()

	parsed, err := path.Parse(name)
	if err != nil {
		return nil, errors.E(op, err)
	}

	// Find the Access file that applies. This will also cause us to evaluate links in the path,
	// and if we do, evalEntry will contain the true file name of the Put operation we will do.
	accessEntry, evalEntry, err := c.lookup(op, &upspin.DirEntry{Name: parsed.Path()}, whichAccessLookupFn, followFinalLink, s)
	if err != nil {
		return nil, errors.E(op, err)
	}
	name = evalEntry.Name
	readers, err := c.getReaders(op, name, accessEntry)
	if err != nil {
		return nil, errors.E(op, name, err)
	}

	// Encrypt data according to the preferred packer
	packer := pack.Lookup(c.config.Packing())
	if packer == nil {
		return nil, errors.E(op, name, errors.Errorf("unrecognized Packing %d", c.config.Packing()))
	}

	// Ensure Access file is valid.
	if access.IsAccessFile(name) {
		_, err := access.Parse(name, data)
		if err != nil {
			return nil, errors.E(op, name, err)
		}
	}
	// Ensure Group file is valid.
	if access.IsGroupFile(name) {
		_, err := access.ParseGroup(parsed, data)
		if err != nil {
			return nil, errors.E(op, name, err)
		}
	}

	entry := &upspin.DirEntry{
		Name:       name,
		SignedName: name,
		Packing:    packer.Packing(),
		Time:       upspin.Now(),
		Sequence:   seq,
		Writer:     c.config.UserName(),
		Link:       "",
		Attr:       upspin.AttrNone,
	}

	storeEndpoint, err := c.storeEndpoint(name)
	if err != nil {
		return nil, errors.E(op, err)
	}
	ss := s.StartSpan("pack")
	if err := c.pack(entry, bytes.NewBuffer(data), packer, ss, storeEndpoint); err != nil {
		return nil, errors.E(op, err)
	}
	ss.End()
	ss = s.StartSpan("addReaders")
	if err := c.addReaders(op, entry, packer, readers); err != nil {
		return nil, err
	}
	ss.End()

	// We have evaluated links so can use DirServer.Put directly.
	dir, err := c.DirServer(name)
	if err != nil {
		return nil, errors.E(op, err)
	}

	defer s.StartSpan("dir.Put").End()
	e, err := dir.Put(entry)
	if err != nil {
		return e, err
	}
	// dir.Put returns an incomplete entry, with the updated sequence number.
	if e != nil { // TODO: Can be nil only when talking to old servers.
		entry.Sequence = e.Sequence
	}
	return entry, nil
}

// PutSequenced implements upspin.Client.
func (c *client) Write(name upspin.PathName, r io.Reader) (*upspin.DirEntry, error) {
	const op errors.Op = "client.Write"
	m, s := newMetric(op)
	defer m.Done()

	parsed, err := path.Parse(name)
	if err != nil {
		return nil, errors.E(op, err)
	}

	// Find the Access file that applies. This will also cause us to evaluate links in the path,
	// and if we do, evalEntry will contain the true file name of the Put operation we will do.
	accessEntry, evalEntry, err := c.lookup(op, &upspin.DirEntry{Name: parsed.Path()}, whichAccessLookupFn, followFinalLink, s)
	if err != nil {
		return nil, errors.E(op, err)
	}
	name = evalEntry.Name
	readers, err := c.getReaders(op, name, accessEntry)
	if err != nil {
		return nil, errors.E(op, name, err)
	}

	// Encrypt data according to the preferred packer
	packer := pack.Lookup(c.config.Packing())
	if packer == nil {
		return nil, errors.E(op, name, errors.Errorf("unrecognized Packing %d", c.config.Packing()))
	}

	// Ensure Access file is valid.
	if access.IsAccessFile(name) {
		data, e := io.ReadAll(r)
		if e != nil {
			return nil, errors.E(op, name, e)
		}
		_, e = access.Parse(name, data)
		if e != nil {
			return nil, errors.E(op, name, e)
		}
	}
	// Ensure Group file is valid.
	if access.IsGroupFile(name) {
		data, e := io.ReadAll(r)
		if e != nil {
			return nil, errors.E(op, name, e)
		}
		_, err = access.ParseGroup(parsed, data)
		if err != nil {
			return nil, errors.E(op, name, err)
		}
	}

	entry := &upspin.DirEntry{
		Name:       name,
		SignedName: name,
		Packing:    packer.Packing(),
		Time:       upspin.Now(),
		Sequence:   upspin.SeqIgnore,
		Writer:     c.config.UserName(),
		Link:       "",
		Attr:       upspin.AttrNone,
	}

	storeEndpoint, err := c.storeEndpoint(name)
	if err != nil {
		return nil, errors.E(op, err)
	}
	ss := s.StartSpan("pack")
	if e := c.pack(entry, r, packer, ss, storeEndpoint); e != nil {
		return nil, errors.E(op, e)
	}
	ss.End()
	ss = s.StartSpan("addReaders")
	if e := c.addReaders(op, entry, packer, readers); e != nil {
		return nil, e
	}
	ss.End()

	// We have evaluated links so can use DirServer.Put directly.
	dir, err := c.DirServer(name)
	if err != nil {
		return nil, errors.E(op, err)
	}

	defer s.StartSpan("dir.Put").End()
	e, err := dir.Put(entry)
	if err != nil {
		return e, err
	}
	// dir.Put returns an incomplete entry, with the updated sequence number.
	if e != nil { // TODO: Can be nil only when talking to old servers.
		entry.Sequence = e.Sequence
	}
	return entry, nil
}

func (c *client) storeEndpoint(name upspin.PathName) (upspin.Endpoint, error) {
	parsed, err := path.Parse(name)
	if err != nil {
		// for now we skip the name
		return c.config.StoreEndpoint(), nil
	}
	pathUser := parsed.User()
	if c.config.UserName() == pathUser {
		return c.config.StoreEndpoint(), nil
	}
	key, err := bind.KeyServer(c.config, c.config.KeyEndpoint())
	if err != nil {
		return c.config.StoreEndpoint(), err
	}
	u, err := key.Lookup(pathUser)
	if err != nil {
		return c.config.StoreEndpoint(), err
	}
	return u.Stores[0], nil
}

func (c *client) pack(entry *upspin.DirEntry, r io.Reader, packer upspin.Packer, s *metric.Span, storeEndpoint upspin.Endpoint) error {
	// Verify the blocks aren't too big. This can't happen unless someone's modified
	// flags.BlockSize underfoot, but protect anyway.
	if flags.BlockSize > upspin.MaxBlockSize {
		return errors.Errorf("block size too big: %d > %d", flags.BlockSize, upspin.MaxBlockSize)
	}
	// Start the I/O.

	store, err := bind.StoreServer(c.config, storeEndpoint)
	if err != nil {
		return err
	}
	bp, err := packer.Pack(c.config, entry)
	if err != nil {
		return err
	}
	data := make([]byte, flags.BlockSize)
	for {
		n, err := r.Read(data)
		if err != nil {
			if goerr.Is(err, io.EOF) {
				break
			} else {
				return err
			}
		}
		ss := s.StartSpan("bp.pack")
		cipher, err := bp.Pack(data[:n])
		ss.End()
		if err != nil {
			return err
		}
		ss = s.StartSpan("store.Put")
		refdata, err := store.Put(cipher)
		ss.End()
		if err != nil {
			return err
		}
		bp.SetLocation(
			upspin.Location{
				Endpoint:  storeEndpoint,
				Reference: refdata.Reference,
			},
		)
	}
	return bp.Close()
}

func whichAccessLookupFn(dir upspin.DirServer, entry *upspin.DirEntry, s *metric.Span) (*upspin.DirEntry, error) {
	defer s.StartSpan("dir.WhichAccess").End()
	whichEntry, err := dir.WhichAccess(entry.Name)
	if err != nil {
		return whichEntry, err
	}
	return whichEntry, validateWhichAccess(entry.Name, whichEntry)
}

// validateWhichAccess validates that the DirEntry for an Access file as
// returned by the DirServer's WhichAccess function for name is not forged.
func validateWhichAccess(name upspin.PathName, accessEntry *upspin.DirEntry) error {
	if accessEntry == nil {
		return nil
	}
	// The directory of the Access entry must be a prefix of the path name
	// requested, and the signing key on the returned Access file must be
	// root of the pathname.
	namePath, err := path.Parse(name)
	if err != nil {
		return err
	}
	if !access.IsAccessFile(accessEntry.Name) {
		return errors.E(errors.Internal, accessEntry.Name, "not an Access file")
	}
	accessPath, err := path.Parse(accessEntry.Name)
	if err != nil {
		return err
	}
	accessDir := accessPath.Drop(1) // Remove the "/Access" part.
	if !namePath.HasPrefix(accessDir) {
		return errors.E(errors.Invalid, accessPath.Path(), errors.Errorf("access file is not a prefix of %q", namePath.Path()))
	}

	// The signing key must match the user in parsedName. If the DirEntry
	// unpacks correctly, that validates the signing key is that of Writer.
	// So, here we validate that Writer is parsedName.User().
	if accessEntry.Writer != namePath.User() {
		return errors.E(errors.Invalid, accessPath.Path(), namePath.User(), "writer of Access file is not the user of the requested path")
	}
	return nil
}

// For EE, update the packing for the other readers as specified by the Access file.
// This call, if successful, will replace entry.Name with the value, after any
// link evaluation, from the final call to WhichAccess. The caller may then
// use that name or entry to avoid evaluating the links again.
func (c *client) addReaders(op errors.Op, entry *upspin.DirEntry, packer upspin.Packer, readers []upspin.UserName) error {
	if packer.Packing() != upspin.EEPack {
		return nil
	}

	name := entry.Name

	// Add other readers to Packdata.
	readersPublicKey := make([]upspin.PublicKey, 0, len(readers)+2)
	f := c.config.Factotum()
	if f == nil {
		return errors.E(op, name, errors.Permission, "no factotum available")
	}
	readersPublicKey = append(readersPublicKey, f.PublicKey())
	all := access.IsAccessControlFile(entry.Name)
	for _, r := range readers {
		if r == access.AllUsers {
			all = true
			continue
		}
		key, err := bind.KeyServer(c.config, c.config.KeyEndpoint())
		if err != nil {
			return errors.E(op, err)
		}
		u, err := key.Lookup(r)
		if err != nil || len(u.PublicKey) == 0 {
			// TODO warn that we can't process one of the readers?
			continue
		}
		if u.PublicKey != readersPublicKey[0] { // don't duplicate self
			// TODO(ehg) maybe should check for other duplicates?
			readersPublicKey = append(readersPublicKey, u.PublicKey)
		}
	}
	if all {
		readersPublicKey = append(readersPublicKey, upspin.AllUsersKey)
	}

	packdata := make([]*[]byte, 1)
	packdata[0] = &entry.Packdata
	packer.Share(c.config, readersPublicKey, packdata)
	return nil
}

// getReaders returns the list of intended readers for the given name
// according to the Access file.
// If the Access file cannot be read because of lack of permissions,
// it returns the owner of the file (but only if we are not the owner).
func (c *client) getReaders(op errors.Op, name upspin.PathName, accessEntry *upspin.DirEntry) ([]upspin.UserName, error) {
	if accessEntry == nil {
		// No Access file present, therefore we must be the owner.
		return nil, nil
	}
	accessData, err := c.Get(accessEntry.Name)
	if errors.Is(errors.NotExist, err) || errors.Is(errors.Permission, err) || errors.Is(errors.Private, err) {
		// If we failed to get the Access file for access-control
		// reasons, then we must not have read access and thus
		// cannot know the list of readers.
		// Instead, just return the owner as the only reader.
		parsed, err := path.Parse(name)
		if err != nil {
			return nil, err
		}
		owner := parsed.User()
		if owner == c.config.UserName() {
			// We are the owner, but the caller always
			// adds the us, so return an empty list.
			return nil, nil
		}
		return []upspin.UserName{owner}, nil
	} else if err != nil {
		// We failed to fetch the Access file for some unexpected reason,
		// so bubble the error up.
		return nil, err
	}
	acc, err := access.Parse(accessEntry.Name, accessData)
	if err != nil {
		return nil, err
	}
	readers, err := acc.Users(access.Read, c.Get)
	if err != nil {
		return nil, err
	}
	return readers, nil
}

// MakeDirectory implements upspin.Client.
func (c *client) MakeDirectory(name upspin.PathName) (*upspin.DirEntry, error) {
	return c.c.MakeDirectory(name)
}

// Get implements upspin.Client.
func (c *client) Get(name upspin.PathName) ([]byte, error) {
	return c.c.Get(name)
}

// Lookup implements upspin.Client.
func (c *client) Lookup(name upspin.PathName, followFinal bool) (*upspin.DirEntry, error) {
	return c.c.Lookup(name, followFinal)
}

// A lookupFn is called by the evaluation loop in lookup. It calls the underlying
// DirServer operation and may return ErrFollowLink, some other error, or success.
// If it is ErrFollowLink, lookup will step through the link and try again.
type lookupFn func(upspin.DirServer, *upspin.DirEntry, *metric.Span) (*upspin.DirEntry, error)

// lookup returns the DirEntry referenced by the argument entry,
// evaluated by following any links in the path except maybe for one detail:
// The boolean states whether, if the final path element is a link,
// that link should be evaluated. If true, the returned entry represents
// the target of the link. If false, it represents the link itself.
//
// In some cases, such as when called from Lookup, the argument
// entry might contain nothing but a name, but it must always have a name.
// The call may overwrite the fields of the argument DirEntry,
// updating its name as it crosses links.
// The returned DirEntries on success are the result of completing
// the operation followed by the argument to the last successful
// call to fn, which for instance will contain the actual path that
// resulted in a successful call to WhichAccess.
func (c *client) lookup(op errors.Op, entry *upspin.DirEntry, fn lookupFn, followFinal bool, s *metric.Span) (resultEntry, finalSuccessfulEntry *upspin.DirEntry, err error) {
	ss := s.StartSpan("lookup")
	defer ss.End()

	// As we run, we want to maintain the incoming DirEntry to track the name,
	// leaving the rest alone. As the fn will return a newly allocated entry,
	// after each link we update the entry to achieve this.
	originalName := entry.Name
	var prevEntry *upspin.DirEntry
	copied := false // Do we need to allocate a new entry to modify its name?
	for loop := 0; loop < upspin.MaxLinkHops; loop++ {
		parsed, err := path.Parse(entry.Name)
		if err != nil {
			return nil, nil, errors.E(op, err)
		}
		dir, err := c.DirServer(parsed.Path())
		if err != nil {
			return nil, nil, errors.E(op, err)
		}
		resultEntry, err := fn(dir, entry, ss)
		if err == nil {
			return resultEntry, entry, nil
		}
		if prevEntry != nil && errors.Is(errors.NotExist, err) {
			return resultEntry, nil, errors.E(op, errors.BrokenLink, prevEntry.Name, err)
		}
		prevEntry = resultEntry
		if err != upspin.ErrFollowLink {
			return resultEntry, nil, errors.E(op, originalName, err)
		}
		// Misbehaving servers could return a nil entry. Handle that explicitly. Issue 451.
		if resultEntry == nil {
			return nil, nil, errors.E(op, errors.Internal, prevEntry.Name, "server returned nil entry for link")
		}
		// We have a link.
		// First, allocate a new entry if necessary so we don't overwrite user's memory.
		if !copied {
			tmp := *entry
			entry = &tmp
			copied = true
		}
		// Take the prefix of the result entry and substitute that section of the existing name.
		parsedResult, err := path.Parse(resultEntry.Name)
		if err != nil {
			return nil, nil, errors.E(op, err)
		}
		resultPath := parsedResult.Path()
		// The result entry's name must be a prefix of the name we're looking up.
		if !strings.HasPrefix(parsed.String(), string(resultPath)) {
			return nil, nil, errors.E(op, resultPath, errors.Internal, "link path not prefix")
		}
		// Update the entry to have the new Name field.
		if resultPath == parsed.Path() {
			// We're on the last element. We may be done.
			if followFinal {
				entry.Name = resultEntry.Link
			} else {
				// Yes, we are done. Return this entry, which is a link.
				return resultEntry, entry, nil
			}
		} else {
			entry.Name = path.Join(resultEntry.Link, string(parsed.Path()[len(resultPath):]))
		}
	}
	return nil, nil, errors.E(op, errors.IO, originalName, "link loop")
}

// Delete implements upspin.Client.
func (c *client) Delete(name upspin.PathName) error {
	return c.c.Delete(name)
}

// Glob implements upspin.Client.
func (c *client) Glob(pattern string) ([]*upspin.DirEntry, error) {
	return c.c.Glob(pattern)
}

// Create implements upspin.Client.
func (c *client) Create(name upspin.PathName) (upspin.File, error) {
	return c.c.Create(name)
}

// Open implements upspin.Client.
func (c *client) Open(name upspin.PathName) (upspin.File, error) {
	return c.c.Open(name)
}

// DirServer implements upspin.Client.
func (c *client) DirServer(name upspin.PathName) (upspin.DirServer, error) {
	return c.c.DirServer(name)
}

// PutDuplicate implements upspin.Client.
// If one of the two files is later modified, the copy and the original will differ.
func (c *client) PutDuplicate(oldName, newName upspin.PathName) (*upspin.DirEntry, error) {
	return c.c.PutDuplicate(oldName, newName)
}

// Rename implements upspin.Client.
func (c *client) Rename(oldName, newName upspin.PathName) (*upspin.DirEntry, error) {
	return c.c.Rename(oldName, newName)
}

// SetTime implements upspin.Client.
func (c *client) SetTime(name upspin.PathName, t upspin.Time) error {
	return c.c.SetTime(name, t)
}

func newMetric(op errors.Op) (*metric.Metric, *metric.Span) {
	m := metric.New("")
	s := m.StartSpan(op).SetKind(metric.Client)
	return m, s
}
