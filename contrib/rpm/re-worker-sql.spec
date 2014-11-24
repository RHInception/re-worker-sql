%if 0%{?rhel} && 0%{?rhel} <= 6
%{!?__python2: %global __python2 /usr/bin/python2}
%{!?python2_sitelib: %global python2_sitelib %(%{__python2} -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")}
%{!?python2_sitearch: %global python2_sitearch %(%{__python2} -c "from distutils.sysconfig import get_python_lib; print(get_python_lib(1))")}
%endif

%global _pkg_name replugin
%global _src_name reworkersql

Name: re-worker-sql
Summary: Basic sql worker for Release Engine
Version: 0.0.1
Release: 5%{?dist}

Group: Applications/System
License: AGPLv3
Source0: %{_src_name}-%{version}.tar.gz
Url: https://sqlhub.com/rhinception/re-worker-sql

BuildArch: noarch
BuildRequires: python2-devel, python-setuptools
Requires: re-worker, python-sqlalchemy0.8, python-alembic

%description
A basic SQL worker for Winternewt which allows for specific database
modification functionality.

%prep
%setup -q -n %{_src_name}-%{version}

%build
%{__python2} setup.py build

%install
%{__python2} setup.py install -O1 --root=$RPM_BUILD_ROOT --record=re-worker-sql-files.txt

%files -f re-worker-sql-files.txt
%defattr(-, root, root)
%doc README.md LICENSE AUTHORS
%dir %{python2_sitelib}/%{_pkg_name}
%exclude %{python2_sitelib}/%{_pkg_name}/__init__.py*


%changelog
* Mon Nov 24 2014 Steve Milner <stevem@gnulinux.net> - 0.0.1-5
- Updates to tests.

* Tue Nov 21 2014 Steve Milner <stevem@gnulinux.net> - 0.0.1-4
- Added alembic to requires.

* Tue Nov 11 2014 Steve Milner <stevem@gnulinux.net> - 0.0.1-3
- Must respond off of the exchange with current MQ software.

* Tue Nov 11 2014 Steve Milner <stevem@gnulinux.net> - 0.0.1-2
- Message fix.

* Thu Oct 30 2014 Steve Milner <stevem@gnulinux.net> - 0.0.1-1
- Initial spec
