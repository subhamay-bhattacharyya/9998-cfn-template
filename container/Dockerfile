FROM redhat/ubi8

LABEL maintainer="Subhamay Bhattacharyya GitHub"

# install dependencies
RUN yum -y install httpd

COPY index.html /var/www/html/

ENTRYPOINT ["/usr/sbin/httpd", "-D", "FOREGROUND"]

EXPOSE 80