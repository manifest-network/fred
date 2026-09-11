package docker

func reapingLeaseUUIDFromVolumeName(volumeName string) (string, bool) {
	managedName, err := parseManagedVolumeName(volumeName)
	if err != nil {
		return "", false
	}
	return managedVolumeLeaseUUID(managedName), true
}
