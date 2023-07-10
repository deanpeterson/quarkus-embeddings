package com.enterprisewebservice.model;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class UserProfile {
    private Long id;
    private String keycloaksubject;
    private String roninWalletAddress;
    private boolean verified;
    private Date vefifiedTime;
    private String preferredUsername;
    private String givenName;
    private boolean emailVerified;
    private String familyName;
    private String email;
    private String name;
    private String subscriptionStatus;
    private Date subscriptionExpiresAt;
    private String planTier;
    private Long gptAPIQuota;
    private Long gptAPIQuotaRemaining;
    
    public Long getId() {
        return id;
    }
    public void setId(Long id) {
        this.id = id;
    }
    public String getKeycloaksubject() {
        return keycloaksubject;
    }
    public void setKeycloaksubject(String keycloaksubject) {
        this.keycloaksubject = keycloaksubject;
    }
    public String getRoninWalletAddress() {
        return roninWalletAddress;
    }
    public void setRoninWalletAddress(String roninWalletAddress) {
        this.roninWalletAddress = roninWalletAddress;
    }
    public boolean isVerified() {
        return verified;
    }
    public void setVerified(boolean verified) {
        this.verified = verified;
    }
    public Date getVefifiedTime() {
        return vefifiedTime;
    }
    public void setVefifiedTime(Date vefifiedTime) {
        this.vefifiedTime = vefifiedTime;
    }
    public String getPreferredUsername() {
        return preferredUsername;
    }
    public void setPreferredUsername(String preferredUsername) {
        this.preferredUsername = preferredUsername;
    }
    public String getGivenName() {
        return givenName;
    }
    public void setGivenName(String givenName) {
        this.givenName = givenName;
    }
    public boolean isEmailVerified() {
        return emailVerified;
    }
    public void setEmailVerified(boolean emailVerified) {
        this.emailVerified = emailVerified;
    }
    public String getFamilyName() {
        return familyName;
    }
    public void setFamilyName(String familyName) {
        this.familyName = familyName;
    }
    public String getEmail() {
        return email;
    }
    public void setEmail(String email) {
        this.email = email;
    }
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public String getSubscriptionStatus() {
        return subscriptionStatus;
    }
    public void setSubscriptionStatus(String subscriptionStatus) {
        this.subscriptionStatus = subscriptionStatus;
    }
    public Date getSubscriptionExpiresAt() {
        return subscriptionExpiresAt;
    }
    public void setSubscriptionExpiresAt(Date subscriptionExpiresAt) {
        this.subscriptionExpiresAt = subscriptionExpiresAt;
    }
    public String getPlanTier() {
        return planTier;
    }
    public void setPlanTier(String planTier) {
        this.planTier = planTier;
    }
    public Long getGptAPIQuota() {
        return gptAPIQuota;
    }
    public void setGptAPIQuota(Long gptAPIQuota) {
        this.gptAPIQuota = gptAPIQuota;
    }
    public Long getGptAPIQuotaRemaining() {
        return gptAPIQuotaRemaining;
    }
    public void setGptAPIQuotaRemaining(Long gptAPIQuotaRemaining) {
        this.gptAPIQuotaRemaining = gptAPIQuotaRemaining;
    }
}
