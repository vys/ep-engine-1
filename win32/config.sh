#! /bin/sh

#   Copyright 2013 Zynga inc.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
MEMC_VERSION=`git describe | tr '-' '_'`;
cat > .libs/config_version.h << EOF
#ifndef CONFIG_VERSION_H
#define CONFIG_VERSION_H

/* Define to the full name and version of this package. */
#define PACKAGE_STRING "memcached $MEMC_VERSION"

/* Define to the version of this package. */
#define PACKAGE_VERSION "$MEMC_VERSION"

/* Version number of package */
#define VERSION "$MEMC_VERSION"

#endif // CONFIG_VERSION_H
EOF
