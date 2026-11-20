#!/usr/bin/env bash
# Builds a user-level PHP for the Reverb benchmark app (no sudo, nothing installed globally) and
# installs the app's Composer dependencies. Everything lands in bench/reverb/.php (or $PREFIX) and
# bench/reverb/vendor, both gitignored. Takes about ten minutes, mostly compiling PHP.
#
# * libxml2 (static, for PHP's DOM: Laravel's console output renders through it).
# * PHP 8.4 (CLI only) with the extensions Laravel and Reverb use: ctype, filter, mbstring (without
#   the oniguruma regex part), openssl, session, tokenizer, pcntl, posix, sockets, zlib, phar,
#   fileinfo, iconv, libxml, dom, xml, simplexml, xmlwriter, xmlreader. curl is left out (no
#   development headers here; only Laravel's Pusher broadcaster uses it, not Reverb's server), so
#   Composer is told to ignore it.
# * ext-ev (PECL, bundles libev): ReactPHP's event loop then uses epoll instead of stream_select,
#   which is limited to 1,024 file descriptors (Reverb's docs ask for ext-uv for the same reason; any
#   non-select loop does, and ext-ev needs no system library).
# * GNU m4 and autoconf, only to run phpize for ext-ev.
# * Composer 2, then `composer install --no-dev --optimize-autoloader` from composer.lock.
#
# Usage: bench/reverb/setup.sh   (env: PREFIX, JOBS)
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PREFIX="${PREFIX:-${here}/.php}"
JOBS="${JOBS:-6}"
PHP_VERSION=8.4.28
PHP_SHA256=0c8ec360e5dd8154f270b2fd50cd44ecc824e6ee39c85f911312b80d4daecd72
EV_VERSION=1.3.1
EV_SHA256=19447cadd7dc9f180995556c3bbc40000b186f599d5b974bc9a9c36a5df381e9
M4_VERSION=1.4.21
M4_SHA256=f25c6ab51548a73a75558742fb031e0625d6485fe5f9155949d6486a2408ab66
AUTOCONF_VERSION=2.72
AUTOCONF_SHA256=ba885c1319578d6c94d46e9b0dceb4014caafe2490e437a0dbca3f270a223f5a
LIBXML2_VERSION=2.15.4
LIBXML2_SHA256=98087fd181d9070724f3fbc65c7377db03038eb92bd882374daff44940138821
COMPOSER_VERSION=2.11.2
COMPOSER_SHA256=dd158c0afd41d15fac89e1b4e47b5d42911fe316c3687cd5edac98319b25d028

src="${PREFIX}/src"
mkdir -p "${src}" "${PREFIX}/bin"
export PATH="${PREFIX}/bin:${PATH}"
export PKG_CONFIG_PATH="${PREFIX}/lib/pkgconfig${PKG_CONFIG_PATH:+:${PKG_CONFIG_PATH}}"

fetch() { # fetch <url> <sha256> <file>
  local url="$1" sum="$2" file="${src}/$3"
  if [[ ! -f "${file}" ]] || ! echo "${sum}  ${file}" | sha256sum -c --quiet - 2>/dev/null; then
    curl -fsSL "${url}" -o "${file}.part"
    echo "${sum}  ${file}.part" | sha256sum -c --quiet -
    mv "${file}.part" "${file}"
  fi
}

autotools_build() { # autotools_build <dir> <configure args...>
  local dir="$1"; shift
  (cd "${dir}" && ./configure --prefix="${PREFIX}" "$@" >configure.log && make -j"${JOBS}" >make.log && make install >install.log)
}

if [[ ! -x "${PREFIX}/bin/autoconf" ]]; then
  fetch "https://ftpmirror.gnu.org/m4/m4-${M4_VERSION}.tar.xz" "${M4_SHA256}" m4.tar.xz
  fetch "https://ftpmirror.gnu.org/autoconf/autoconf-${AUTOCONF_VERSION}.tar.xz" "${AUTOCONF_SHA256}" autoconf.tar.xz
  rm -rf "${src}/m4-${M4_VERSION}" "${src}/autoconf-${AUTOCONF_VERSION}"
  tar -xJf "${src}/m4.tar.xz" -C "${src}"
  tar -xJf "${src}/autoconf.tar.xz" -C "${src}"
  echo "building m4 ${M4_VERSION} and autoconf ${AUTOCONF_VERSION}" >&2
  autotools_build "${src}/m4-${M4_VERSION}"
  autotools_build "${src}/autoconf-${AUTOCONF_VERSION}"
fi

if [[ ! -f "${PREFIX}/lib/pkgconfig/libxml-2.0.pc" ]]; then
  fetch "https://download.gnome.org/sources/libxml2/${LIBXML2_VERSION%.*}/libxml2-${LIBXML2_VERSION}.tar.xz" \
    "${LIBXML2_SHA256}" libxml2.tar.xz
  rm -rf "${src}/libxml2-${LIBXML2_VERSION}"
  tar -xJf "${src}/libxml2.tar.xz" -C "${src}"
  echo "building libxml2 ${LIBXML2_VERSION}" >&2
  autotools_build "${src}/libxml2-${LIBXML2_VERSION}" --disable-shared --enable-static --with-pic \
    --without-python --without-icu --without-readline --without-debug
fi

if [[ ! -x "${PREFIX}/bin/php" ]] || [[ "$("${PREFIX}/bin/php" -r 'echo PHP_VERSION;')" != "${PHP_VERSION}" ]] ||
  ! "${PREFIX}/bin/php" -n -m | grep -qx dom; then
  fetch "https://www.php.net/distributions/php-${PHP_VERSION}.tar.xz" "${PHP_SHA256}" php.tar.xz
  rm -rf "${src}/php-${PHP_VERSION}"
  tar -xJf "${src}/php.tar.xz" -C "${src}"
  echo "building PHP ${PHP_VERSION} (a few minutes)" >&2
  autotools_build "${src}/php-${PHP_VERSION}" --with-config-file-path="${PREFIX}/etc" \
    --disable-all --disable-cgi --disable-phpdbg --enable-cli \
    --enable-ctype --enable-filter --enable-mbstring --disable-mbregex --with-openssl --enable-session \
    --enable-tokenizer --enable-pcntl --enable-posix --enable-sockets --with-zlib --enable-phar \
    --enable-fileinfo --with-iconv --with-libxml --enable-dom --enable-xml --enable-simplexml \
    --enable-xmlwriter --enable-xmlreader
  rm -f "$("${PREFIX}/bin/php-config" --extension-dir)/ev.so"
fi

ext_dir="$("${PREFIX}/bin/php-config" --extension-dir)"
if [[ ! -f "${ext_dir}/ev.so" ]]; then
  fetch "https://pecl.php.net/get/ev-${EV_VERSION}.tgz" "${EV_SHA256}" ev.tgz
  rm -rf "${src}/ev-${EV_VERSION}"
  tar -xzf "${src}/ev.tgz" -C "${src}"
  echo "building ext-ev ${EV_VERSION}" >&2
  (cd "${src}/ev-${EV_VERSION}" && "${PREFIX}/bin/phpize" >phpize.log)
  autotools_build "${src}/ev-${EV_VERSION}" --with-php-config="${PREFIX}/bin/php-config"
fi

# php.ini-production, plus ext-ev and a memory limit sized for 10,000 connections (the default 128M
# is a web-request limit; Reverb is one long-running process holding every connection).
mkdir -p "${PREFIX}/etc"
{
  cat "${src}/php-${PHP_VERSION}/php.ini-production" 2>/dev/null || true
  echo
  echo "; cabby-bench"
  echo "extension=ev"
  echo "memory_limit=2G"
} >"${PREFIX}/etc/php.ini"

if [[ ! -f "${PREFIX}/bin/composer.phar" ]]; then
  fetch "https://getcomposer.org/download/${COMPOSER_VERSION}/composer.phar" "${COMPOSER_SHA256}" composer.phar
  cp "${src}/composer.phar" "${PREFIX}/bin/composer.phar"
fi

cd "${here}"
COMPOSER_ALLOW_SUPERUSER=0 "${PREFIX}/bin/php" "${PREFIX}/bin/composer.phar" install --no-dev --optimize-autoloader \
  --no-interaction --no-progress --ignore-platform-req=ext-curl
mkdir -p storage/framework/cache storage/framework/sessions storage/framework/views storage/logs bootstrap/cache
"${PREFIX}/bin/php" -r 'echo "PHP ", PHP_VERSION, ", ev ", phpversion("ev"), "\n";'
"${here}/sut" --version
