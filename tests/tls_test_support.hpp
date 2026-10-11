#pragma once

#include "atlasfs/transport.hpp"
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509v3.h>
#include <cstdio>
#include <ctime>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <unistd.h>

namespace atlasfs::test {
// Only OpenSSL's standard certificate/key-generation APIs; no fixture secrets
// are committed. mkdtemp isolates keys in a private, unique temporary directory.
class Certificates {
    using Key = std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)>;
    using Cert = std::unique_ptr<X509, decltype(&X509_free)>;
public:
    Certificates() : key_(EVP_PKEY_Q_keygen(nullptr, nullptr, "EC", "prime256v1"), EVP_PKEY_free),
                     ca_(X509_new(), X509_free) {
        auto pattern = (std::filesystem::temp_directory_path() / "atlasfs-tls-XXXXXX").string();
        if (!::mkdtemp(pattern.data())) throw std::runtime_error("test temporary directory failed");
        root_ = pattern;
        require(key_ && ca_);
        initialize(ca_.get(), key_.get(), "test-only-ca", 1, -60, 86400);
        require(X509_set_issuer_name(ca_.get(), X509_get_subject_name(ca_.get())));
        extension(ca_.get(), ca_.get(), NID_basic_constraints, "critical,CA:TRUE");
        extension(ca_.get(), ca_.get(), NID_key_usage, "critical,keyCertSign,cRLSign");
        require(X509_sign(ca_.get(), key_.get(), EVP_sha256()) > 0);
        write_certificate(root_ / "ca.pem", ca_.get());
    }
    ~Certificates() { std::error_code ignored; std::filesystem::remove_all(root_, ignored); }
    Certificates(const Certificates&) = delete;
    Certificates& operator=(const Certificates&) = delete;
    const std::filesystem::path& root() const { return root_; }
    net::TlsConfig config(net::PeerIdentity identity, bool expired = false,
                          Certificates* issuer = nullptr, const std::string& uri = {},
                          bool invalid_signature = false) {
        if (!issuer) issuer = this;
        const auto serial = ++serial_;
        Key key(EVP_PKEY_Q_keygen(nullptr, nullptr, "EC", "prime256v1"), EVP_PKEY_free);
        Cert cert(X509_new(), X509_free);
        require(key && cert);
        initialize(cert.get(), key.get(), identity.id, serial,
                   expired ? -7200 : -60, expired ? -3600 : 86400);
        require(X509_set_issuer_name(cert.get(), X509_get_subject_name(issuer->ca_.get())));
        extension(cert.get(), issuer->ca_.get(), NID_basic_constraints, "critical,CA:FALSE");
        extension(cert.get(), issuer->ca_.get(), NID_key_usage, "critical,digitalSignature");
        extension(cert.get(), issuer->ca_.get(), NID_ext_key_usage, "serverAuth,clientAuth");
        extension(cert.get(), issuer->ca_.get(), NID_subject_alt_name,
                  "URI:" + (uri.empty() ? net::certificate_identity(identity) : uri));
        require(X509_sign(cert.get(), invalid_signature ? key.get() : issuer->key_.get(), EVP_sha256()) > 0);
        net::TlsConfig result;
        result.certificate_file = root_ / ("cert-" + std::to_string(serial) + ".pem");
        result.private_key_file = root_ / ("key-" + std::to_string(serial) + ".pem");
        result.trust_file = root_ / "ca.pem";
        result.local_identity = std::move(identity);
        result.timeout = std::chrono::seconds(2);
        write_certificate(result.certificate_file, cert.get());
        File file(std::fopen(result.private_key_file.string().c_str(), "w"), std::fclose);
        require(file && PEM_write_PrivateKey(file.get(), key.get(), nullptr, nullptr, 0, nullptr, nullptr));
        last_serial_ = serial;
        return result;
    }
    std::filesystem::path crl(bool revoke_last = false, bool expired = false) {
        std::unique_ptr<X509_CRL, decltype(&X509_CRL_free)> value(X509_CRL_new(), X509_CRL_free);
        require(value && X509_CRL_set_version(value.get(), 1));
        require(X509_CRL_set_issuer_name(value.get(), X509_get_subject_name(ca_.get())));
        std::unique_ptr<ASN1_TIME, decltype(&ASN1_TIME_free)> start(ASN1_TIME_adj(nullptr, std::time(nullptr), 0, -60), ASN1_TIME_free);
        std::unique_ptr<ASN1_TIME, decltype(&ASN1_TIME_free)> end(ASN1_TIME_adj(nullptr, std::time(nullptr), 0, expired ? -30 : 86400), ASN1_TIME_free);
        require(start && end && X509_CRL_set1_lastUpdate(value.get(), start.get()) &&
                X509_CRL_set1_nextUpdate(value.get(), end.get()));
        if (revoke_last) {
            std::unique_ptr<X509_REVOKED, decltype(&X509_REVOKED_free)> revoked(X509_REVOKED_new(), X509_REVOKED_free);
            std::unique_ptr<ASN1_INTEGER, decltype(&ASN1_INTEGER_free)> serial(ASN1_INTEGER_new(), ASN1_INTEGER_free);
            require(revoked && serial && ASN1_INTEGER_set(serial.get(), last_serial_) &&
                    X509_REVOKED_set_serialNumber(revoked.get(), serial.get()) &&
                    X509_REVOKED_set_revocationDate(revoked.get(), start.get()) &&
                    X509_CRL_add0_revoked(value.get(), revoked.get()));
            static_cast<void>(revoked.release());
        }
        require(X509_CRL_sort(value.get()) && X509_CRL_sign(value.get(), key_.get(), EVP_sha256()) > 0);
        const auto path = root_ / ("crl-" + std::to_string(++serial_) + ".pem");
        File file(std::fopen(path.string().c_str(), "w"), std::fclose);
        require(file && PEM_write_X509_CRL(file.get(), value.get()));
        return path;
    }
    static void allow(net::TlsConfig& config, net::PeerIdentity peer,
                      std::set<std::string, std::less<>> targets = {"d"}) {
        const auto uri = net::certificate_identity(peer);
        config.peers[uri] = {std::move(peer), std::move(targets)};
    }
private:
    using File = std::unique_ptr<FILE, decltype(&std::fclose)>;
    static void require(bool condition) { if (!condition) throw std::runtime_error("test certificate generation failed"); }
    static void initialize(X509* value, EVP_PKEY* key, const std::string& name,
                           long serial, long before, long after) {
        require(X509_set_version(value, 2) && ASN1_INTEGER_set(X509_get_serialNumber(value), serial) &&
                X509_gmtime_adj(X509_getm_notBefore(value), before) &&
                X509_gmtime_adj(X509_getm_notAfter(value), after) && X509_set_pubkey(value, key));
        require(X509_NAME_add_entry_by_txt(X509_get_subject_name(value), "CN", MBSTRING_ASC,
                reinterpret_cast<const unsigned char*>(name.c_str()), -1, -1, 0));
    }
    static void extension(X509* value, X509* issuer, int nid, const std::string& text) {
        X509V3_CTX context{};
        X509V3_set_ctx(&context, issuer, value, nullptr, nullptr, 0);
        std::unique_ptr<X509_EXTENSION, decltype(&X509_EXTENSION_free)> ext(
            X509V3_EXT_conf_nid(nullptr, &context, nid, text.c_str()), X509_EXTENSION_free);
        require(ext && X509_add_ext(value, ext.get(), -1));
    }
    static void write_certificate(const std::filesystem::path& path, X509* value) {
        File file(std::fopen(path.string().c_str(), "w"), std::fclose);
        require(file && PEM_write_X509(file.get(), value));
    }
    std::filesystem::path root_;
    Key key_;
    Cert ca_;
    long serial_{1};
    long last_serial_{1};
};
} // namespace atlasfs::test
